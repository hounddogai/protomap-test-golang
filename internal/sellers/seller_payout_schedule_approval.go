package sellers

import (
	"context"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	experimentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/experiments/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// SellerPayoutScheduleApprovalServer implements `acme.sellers.v1.SellerPayoutScheduleApprovalService`.
// Routes seller payout schedules through approval, with escalation and delegation.
type SellerPayoutScheduleApprovalServer struct {
	sellersv1.UnimplementedSellerPayoutScheduleApprovalServiceServer
	store                              *Store
	log                                *slog.Logger
	conn                               *grpc.ClientConn
	sellerListingReportingClient       sellersv1.SellerListingReportingServiceClient
	preferenceValidationClient         customersv1.PreferenceValidationServiceClient
	giftCardWorkflowClient             giftcardsv1.GiftCardWorkflowServiceClient
	sellerScorecardSyncClient          sellersv1.SellerScorecardSyncServiceClient
	giftCardApprovalClient             giftcardsv1.GiftCardApprovalServiceClient
	sellerScorecardLifecycleClient     sellersv1.SellerScorecardLifecycleServiceClient
	experimentQueryClient              experimentsv1.ExperimentQueryServiceClient
	sellerWorkflowClient               sellersv1.SellerWorkflowServiceClient
	consentRecordValidationClient      customersv1.ConsentRecordValidationServiceClient
	householdCalculationClient         customersv1.HouseholdCalculationServiceClient
	recalculateConsentRecordsFn        func(context.Context, *customersv1.RecalculateConsentRecordsRequest, ...grpc.CallOption) (*customersv1.RecalculateConsentRecordsResponse, error)
	sellerListingApprovalClient        sellersv1.SellerListingApprovalServiceClient
	sellerApprovalClient               sellersv1.SellerApprovalServiceClient
	meteredGiftCardTransactionWorkflow meteredGiftCardTransactionWorkflow
	householdQueryClient               customersv1.HouseholdQueryServiceClient
	bus                                *eventbus.Bus
	sellerPayoutScheduleReportingPort  sellerPayoutScheduleReportingPort
}

// NewSellerPayoutScheduleApprovalServer wires a SellerPayoutScheduleApprovalServer.
func NewSellerPayoutScheduleApprovalServer(deps platform.Deps) *SellerPayoutScheduleApprovalServer {
	consentRecordCalculation := customersv1.NewConsentRecordCalculationServiceClient(deps.Conn)
	return &SellerPayoutScheduleApprovalServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.sellers.v1.SellerPayoutScheduleApprovalService"),
		conn:                               deps.Conn,
		sellerListingReportingClient:       sellersv1.NewSellerListingReportingServiceClient(deps.Conn),
		preferenceValidationClient:         customersv1.NewPreferenceValidationServiceClient(deps.Conn),
		giftCardWorkflowClient:             giftcardsv1.NewGiftCardWorkflowServiceClient(deps.Conn),
		sellerScorecardSyncClient:          sellersv1.NewSellerScorecardSyncServiceClient(deps.Conn),
		giftCardApprovalClient:             giftcardsv1.NewGiftCardApprovalServiceClient(deps.Conn),
		sellerScorecardLifecycleClient:     sellersv1.NewSellerScorecardLifecycleServiceClient(deps.Conn),
		experimentQueryClient:              experimentsv1.NewExperimentQueryServiceClient(deps.Conn),
		sellerWorkflowClient:               sellersv1.NewSellerWorkflowServiceClient(deps.Conn),
		consentRecordValidationClient:      customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		householdCalculationClient:         customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
		recalculateConsentRecordsFn:        consentRecordCalculation.RecalculateConsentRecords,
		sellerListingApprovalClient:        sellersv1.NewSellerListingApprovalServiceClient(deps.Conn),
		sellerApprovalClient:               sellersv1.NewSellerApprovalServiceClient(deps.Conn),
		meteredGiftCardTransactionWorkflow: meteredGiftCardTransactionWorkflow{GiftCardTransactionWorkflowServiceClient: giftcardsv1.NewGiftCardTransactionWorkflowServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		householdQueryClient:               customersv1.NewHouseholdQueryServiceClient(deps.Conn),
		bus:                                deps.Bus,
		sellerPayoutScheduleReportingPort:  newSellerPayoutScheduleReportingPort(deps),
	}
}

// SubmitSellerPayoutSchedule submits a seller payout schedule.
func (s *SellerPayoutScheduleApprovalServer) SubmitSellerPayoutSchedule(ctx context.Context, req *sellersv1.SubmitSellerPayoutScheduleRequest) (*sellersv1.SubmitSellerPayoutScheduleResponse, error) {
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "submit seller payout schedule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetSellerPayoutSchedule())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	cancelGiftCardTransactionWorkflowRequest := &giftcardsv1.CancelGiftCardTransactionWorkflowRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.CancelGiftCardTransactionWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{Detail: packed},
	}
	cancelGiftCardTransactionWorkflowReply := new(giftcardsv1.CancelGiftCardTransactionWorkflowResponse)
	if err := grpc.Invoke(ctx, "/acme.giftcards.v1.GiftCardTransactionWorkflowService/CancelGiftCardTransactionWorkflow", cancelGiftCardTransactionWorkflowRequest, cancelGiftCardTransactionWorkflowReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionWorkflowService/CancelGiftCardTransactionWorkflow")
	}
	for participant := range iterx.Each(req.GetSellerPayoutSchedule().GetParticipants(), func(candidate *sellersv1.SellerPayoutScheduleParticipant) bool { return candidate != nil }) {
		if _, err := s.sellerListingReportingClient.ListSellerListingReports(ctx, &sellersv1.ListSellerListingReportsRequest{
			Context: req.GetContext(),
			Filter: &sellersv1.ListSellerListingReportsRequest_Filter{
				Region:       req.GetSellerPayoutSchedule().GetLocation().GetRegion(),
				OwnerId:      participant.GetParticipantId(),
				CreatedAfter: req.GetSellerPayoutSchedule().GetAudit().GetCreatedAt(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/ListSellerListingReports")
		}
	}
	ctx = tenancy.With(ctx, "created_by", req.GetSellerPayoutSchedule().GetAudit().GetCreatedBy())
	validatePreferenceRequest := &customersv1.ValidatePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				UserId:      req.GetDetails().GetApprover().GetUserId(),
				DisplayName: cancelGiftCardTransactionWorkflowReply.GetGiftCardTransaction().GetSubject().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: tenancy.Value(ctx, "created_by"),
				UpdatedBy: req.GetSellerPayoutSchedule().GetAudit().GetUpdatedBy(),
			},
		},
	}
	validatePreferenceReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*customersv1.ValidatePreferenceResponse, error) {
		return s.preferenceValidationClient.ValidatePreference(ctx, validatePreferenceRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/ValidatePreference")
	}
	signalGiftCardWorkflowRequest := newSignalGiftCardWorkflowRequest(withSignalGiftCardWorkflowContext(req.GetContext()), withSignalGiftCardWorkflowGiftCard(&giftcardsv1.GiftCard{
		Audit: &commonv1.AuditInfo{
			CreatedBy: validatePreferenceReply.GetPreference().GetAudit().GetCreatedBy(),
		},
	}))
	signalGiftCardWorkflowReply, err := s.giftCardWorkflowClient.SignalGiftCardWorkflow(ctx, signalGiftCardWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardWorkflowService/SignalGiftCardWorkflow")
	}
	return &sellersv1.SubmitSellerPayoutScheduleResponse{
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Organizer: &identityv1.UserRef{
				DisplayName: cancelGiftCardTransactionWorkflowReply.GetGiftCardTransaction().GetSubject().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: validatePreferenceReply.GetPreference().GetAudit().GetCreatedAt(),
				CreatedBy: validatePreferenceReply.GetPreference().GetAudit().GetCreatedBy(),
			},
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Window:                 req.GetSellerPayoutSchedule().GetWindow(),
		},
		Outcome: &sellersv1.SubmitSellerPayoutScheduleResponse_Outcome{
			ReferenceId: cancelGiftCardTransactionWorkflowReply.GetOutcome().GetReferenceId(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: signalGiftCardWorkflowReply.GetResult().GetReferenceIds(),
			Status:       signalGiftCardWorkflowReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// ApproveSellerPayoutSchedule approves a seller payout schedule.
func (s *SellerPayoutScheduleApprovalServer) ApproveSellerPayoutSchedule(ctx context.Context, req *sellersv1.ApproveSellerPayoutScheduleRequest) (*sellersv1.ApproveSellerPayoutScheduleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "approve seller payout schedule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	sellerPayoutScheduleCache := cache.New[string, *sellersv1.SellerPayoutSchedule](1)
	sellerPayoutScheduleCache.Put(req.GetContext().GetTraceId(), req.GetSellerPayoutSchedule())
	cachedSellerPayoutSchedule, _ := sellerPayoutScheduleCache.Get(req.GetContext().GetTraceId())
	snapshot, err := protojson.Marshal(req.GetSellerPayoutSchedule())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	resolveSellerScorecardConflictRequest := &sellersv1.ResolveSellerScorecardConflictRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			Audit: &commonv1.AuditInfo{
				CreatedBy: cachedSellerPayoutSchedule.GetAudit().GetCreatedBy(),
			},
		},
		Details: &sellersv1.ResolveSellerScorecardConflictRequest_Details{Payload: snapshot},
	}
	send := s.resolveSellerScorecardConflictSender(resolveSellerScorecardConflictRequest)
	resolveSellerScorecardConflictReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardSyncService/ResolveSellerScorecardConflict")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*giftcardsv1.SubmitGiftCardResponse, len(req.GetSellerPayoutSchedule().GetParticipants()))
	for i, participant := range req.GetSellerPayoutSchedule().GetParticipants() {
		group.Go(func() error {
			reply, err := s.giftCardApprovalClient.SubmitGiftCard(groupCtx, &giftcardsv1.SubmitGiftCardRequest{
				Context: req.GetContext(),
				GiftCard: &giftcardsv1.GiftCard{
					Contact: &commonv1.ContactInfo{
						Name:  participant.GetContact().GetName(),
						Phone: participant.GetContact().GetPhone(),
					},
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
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardApprovalService/SubmitGiftCard")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	for _, participant2 := range req.GetSellerPayoutSchedule().GetParticipants() {
		if _, err := s.sellerScorecardLifecycleClient.ScheduleSellerScorecardExpiry(ctx, &sellersv1.ScheduleSellerScorecardExpiryRequest{
			Context: req.GetContext(),
			Details: &sellersv1.ScheduleSellerScorecardExpiryRequest_Details{
				Notify: &commonv1.ContactInfo{
					Email:            participant2.GetContact().GetEmail(),
					Phone:            participant2.GetContact().GetPhone(),
					Name:             participant2.GetContact().GetName(),
					PreferredChannel: participant2.GetContact().GetPreferredChannel(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardLifecycleService/ScheduleSellerScorecardExpiry")
		}
	}
	return &sellersv1.ApproveSellerPayoutScheduleResponse{
		Outcome: &sellersv1.ApproveSellerPayoutScheduleResponse_Outcome{
			Status: resolveSellerScorecardConflictReply.GetOutcome().GetStatus(),
		},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Title:                  req.GetSellerPayoutSchedule().GetTitle(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EscalateSellerPayoutSchedule escalates a seller payout schedule.
func (s *SellerPayoutScheduleApprovalServer) EscalateSellerPayoutSchedule(ctx context.Context, req *sellersv1.EscalateSellerPayoutScheduleRequest) (*sellersv1.EscalateSellerPayoutScheduleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EscalateSellerPayoutSchedule", "error", err)
		return nil, err
	}
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "escalate seller payout schedule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	resolveExperimentsRequest := buildResolveExperimentsRequestForEscalateSellerPayoutSchedule(req)
	resolveExperimentsReply, err := s.experimentQueryClient.ResolveExperiments(ctx, resolveExperimentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.experiments.v1.ExperimentQueryService/ResolveExperiments")
	}
	return &sellersv1.EscalateSellerPayoutScheduleResponse{
		Result: &commonv1.OperationResult{
			Details: resolveExperimentsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Metadata:               req.GetSellerPayoutSchedule().GetMetadata(),
		},
	}, nil
}

// ListPendingSellerPayoutSchedules lists seller payout schedules a page at a time.
func (s *SellerPayoutScheduleApprovalServer) ListPendingSellerPayoutSchedules(ctx context.Context, req *sellersv1.ListPendingSellerPayoutSchedulesRequest) (*sellersv1.ListPendingSellerPayoutSchedulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListPendingSellerPayoutSchedules", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending seller payout schedules", "tenant", req.GetContext().GetTenantId())
	validatePreferenceRequest := buildValidatePreferenceRequestForListPendingSellerPayoutSchedules(req)
	validatePreferenceReply, err := s.preferenceValidationClient.ValidatePreference(ctx, validatePreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/ValidatePreference")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := sellerPayoutScheduleApprovalServerAdvanceSellerWorkflowSteps[mode]
	advanceSellerWorkflowRequest := &sellersv1.AdvanceSellerWorkflowRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			Audit: &commonv1.AuditInfo{
				Etag: validatePreferenceReply.GetPreference().GetAudit().GetEtag(),
			},
		},
	}
	advanceSellerWorkflowReply, err := run(s, ctx, advanceSellerWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerWorkflowService/AdvanceSellerWorkflow")
	}
	return &sellersv1.ListPendingSellerPayoutSchedulesResponse{
		SellerPayoutSchedules: []*sellersv1.SellerPayoutSchedule{&sellersv1.SellerPayoutSchedule{
			Audit: &commonv1.AuditInfo{
				Etag: validatePreferenceReply.GetPreference().GetAudit().GetEtag(),
			},
		}},
		Result: &commonv1.OperationResult{
			Details: validatePreferenceReply.GetResult().GetDetails(),
			Message: advanceSellerWorkflowReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// DelegateSellerPayoutScheduleApproval delegates a seller payout schedule.
func (s *SellerPayoutScheduleApprovalServer) DelegateSellerPayoutScheduleApproval(ctx context.Context, req *sellersv1.DelegateSellerPayoutScheduleApprovalRequest) (*sellersv1.DelegateSellerPayoutScheduleApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "delegate seller payout schedule approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetSellerPayoutSchedule()).(*sellersv1.SellerPayoutSchedule)
	packed, err := anypb.New(req.GetSellerPayoutSchedule())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	validateConsentRecordRequest := &customersv1.ValidateConsentRecordRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			Detail: packed,
		},
		Details: &customersv1.ValidateConsentRecordRequest_Details{
			Contact: &commonv1.ContactInfo{Email: cloned.GetOrganizer().GetEmail()},
		},
	}
	validateConsentRecordReply, err := grpcx.Call(ctx, s.consentRecordValidationClient.ValidateConsentRecord, validateConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ValidateConsentRecord")
	}
	simulateHouseholdRequest := &customersv1.SimulateHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Children: []*customersv1.Household{&customersv1.Household{
						HouseholdId: validateConsentRecordReply.GetConsentRecord().GetSubject().GetHouseholdId(),
					}},
				}},
			}},
		},
	}
	simulateHouseholdReply, err := customersv1.HouseholdCalculationServiceClient.SimulateHousehold(s.householdCalculationClient, ctx, simulateHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/SimulateHousehold")
	}
	return &sellersv1.DelegateSellerPayoutScheduleApprovalResponse{
		Result: &commonv1.OperationResult{Status: validateConsentRecordReply.GetResult().GetStatus()},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Location: &commonv1.PostalAddress{
				CountryCode: simulateHouseholdReply.GetHousehold().GetLocation().GetCountryCode(),
			},
			Organizer: &identityv1.UserRef{
				UserId: simulateHouseholdReply.GetHousehold().GetManager().GetUserId(),
			},
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Title:                  req.GetSellerPayoutSchedule().GetTitle(),
		},
	}, nil
}

// RecallSellerPayoutSchedule recalls a seller payout schedule.
func (s *SellerPayoutScheduleApprovalServer) RecallSellerPayoutSchedule(ctx context.Context, req *sellersv1.RecallSellerPayoutScheduleRequest) (*sellersv1.RecallSellerPayoutScheduleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecallSellerPayoutSchedule", "error", err)
		return nil, err
	}
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "recall seller payout schedule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetSellerPayoutSchedule())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	recalculateConsentRecordsRequest := &customersv1.RecalculateConsentRecordsRequest{
		Context: req.GetContext(),
		Details: &customersv1.RecalculateConsentRecordsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region:   req.GetSellerPayoutSchedule().GetLocation().GetRegion(),
				Locality: req.GetSellerPayoutSchedule().GetLocation().GetLocality(),
				Lines:    req.GetSellerPayoutSchedule().GetLocation().GetLines(),
			},
		},
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	recalculateConsentRecordsReply, err := s.recalculateConsentRecordsFn(ctx, recalculateConsentRecordsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/RecalculateConsentRecords")
	}
	jobs := make(chan *sellersv1.SellerPayoutScheduleParticipant)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for participant := range jobs {
				if _, err := s.sellerListingApprovalClient.DelegateSellerListingApproval(workCtx, &sellersv1.DelegateSellerListingApprovalRequest{
					Context: req.GetContext(),
					Details: &sellersv1.DelegateSellerListingApprovalRequest_Details{
						Approver: &identityv1.UserRef{Email: participant.GetContact().GetEmail()},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetSellerPayoutSchedule().GetParticipants() {
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
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingApprovalService/DelegateSellerListingApproval")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*sellersv1.DelegateSellerApprovalResponse, len(req.GetSellerPayoutSchedule().GetParticipants()))
	for i, participant2 := range req.GetSellerPayoutSchedule().GetParticipants() {
		group.Go(func() error {
			reply, err := s.sellerApprovalClient.DelegateSellerApproval(groupCtx, &sellersv1.DelegateSellerApprovalRequest{
				Context: req.GetContext(),
				Details: &sellersv1.DelegateSellerApprovalRequest_Details{
					Approver: &identityv1.UserRef{Email: participant2.GetContact().GetEmail()},
				},
				Seller: &sellersv1.Seller{
					Contact: &commonv1.ContactInfo{Phone: participant2.GetContact().GetPhone()},
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
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerApprovalService/DelegateSellerApproval")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	resumeGiftCardTransactionWorkflowRequest := &giftcardsv1.ResumeGiftCardTransactionWorkflowRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: req.GetSellerPayoutSchedule().GetOrganizer().GetDisplayName(),
			},
		},
	}
	resumeGiftCardTransactionWorkflowReply, err := s.meteredGiftCardTransactionWorkflow.ResumeGiftCardTransactionWorkflow(ctx, resumeGiftCardTransactionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionWorkflowService/ResumeGiftCardTransactionWorkflow")
	}
	return &sellersv1.RecallSellerPayoutScheduleResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: recalculateConsentRecordsReply.GetResult().GetCorrelationId(),
			Message:       recalculateConsentRecordsReply.GetResult().GetMessage(),
			Status:        resumeGiftCardTransactionWorkflowReply.GetResult().GetStatus(),
		},
		Outcome: &sellersv1.RecallSellerPayoutScheduleResponse_Outcome{
			Message: resumeGiftCardTransactionWorkflowReply.GetResult().GetMessage(),
		},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Organizer:              req.GetSellerPayoutSchedule().GetOrganizer(),
		},
	}, nil
}

// GetSellerPayoutScheduleApprovalStatus returns a seller payout schedule.
func (s *SellerPayoutScheduleApprovalServer) GetSellerPayoutScheduleApprovalStatus(ctx context.Context, req *sellersv1.GetSellerPayoutScheduleApprovalStatusRequest) (*sellersv1.GetSellerPayoutScheduleApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_payout_schedule", req.GetSellerPayoutScheduleId(), "get_seller_payout_schedule_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get seller payout schedule approval status", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "seller_payout_schedule_id", req.GetSellerPayoutScheduleId())
	countHouseholdsRequest := &customersv1.CountHouseholdsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.CountHouseholdsRequest_Filter{
			OwnerId:      tenancy.Value(ctx, "seller_payout_schedule_id"),
			CreatedAfter: req.GetContext().GetDeadline(),
			Labels:       req.GetContext().GetBaggage(),
		},
	}
	type countHouseholdsResult struct {
		reply *customersv1.CountHouseholdsResponse
		err   error
	}
	countHouseholdsResults := make(chan countHouseholdsResult, 1)
	go func() {
		reply, err := s.householdQueryClient.CountHouseholds(ctx, countHouseholdsRequest)
		countHouseholdsResults <- countHouseholdsResult{reply, err}
	}()
	var countHouseholdsReply *customersv1.CountHouseholdsResponse
	select {
	case result := <-countHouseholdsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.HouseholdQueryService/CountHouseholds")
		}
		countHouseholdsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &sellersv1.GetSellerPayoutScheduleApprovalStatusResponse{
		Result: &commonv1.OperationResult{
			Details: countHouseholdsReply.GetResult().GetDetails(),
			Status:  countHouseholdsReply.GetResult().GetStatus(),
		},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{SellerPayoutScheduleId: req.GetSellerPayoutScheduleId()},
	}, nil
}

// CommentSellerPayoutScheduleApproval comments a seller payout schedule.
func (s *SellerPayoutScheduleApprovalServer) CommentSellerPayoutScheduleApproval(ctx context.Context, req *sellersv1.CommentSellerPayoutScheduleApprovalRequest) (*sellersv1.CommentSellerPayoutScheduleApprovalResponse, error) {
	if err := s.store.SaveSellerPayoutSchedule(ctx, req.GetContext().GetTenantId(), req.GetSellerPayoutSchedule()); err != nil {
		return nil, grpcx.Wrap(err, "save seller payout schedule")
	}
	s.log.InfoContext(ctx, "comment seller payout schedule approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerPayoutSchedule().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "sellers.comment_seller_payout_schedule_approval.compare_seller_listing_options",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"seller_listing.address.locality": req.GetSellerPayoutSchedule().GetLocation().GetLocality(),
			"context.tenant_id":               req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	countSellerPayoutScheduleTotalsRequest := &sellersv1.CountSellerPayoutScheduleTotalsRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.CountSellerPayoutScheduleTotalsRequest_Filter{
			CreatedAfter: req.GetSellerPayoutSchedule().GetAudit().GetUpdatedAt(),
		},
	}
	countSellerPayoutScheduleTotalsReply, err := s.sellerPayoutScheduleReportingPort.CountSellerPayoutScheduleTotals(ctx, countSellerPayoutScheduleTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleReportingService/CountSellerPayoutScheduleTotals")
	}
	return &sellersv1.CommentSellerPayoutScheduleApprovalResponse{
		Result: &commonv1.OperationResult{
			Status: countSellerPayoutScheduleTotalsReply.GetResult().GetStatus(),
		},
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			SellerPayoutScheduleId: req.GetSellerPayoutScheduleId(),
			Location:               req.GetSellerPayoutSchedule().GetLocation(),
		},
	}, nil
}

// resolveSellerScorecardConflictSender returns a function that sends one request to
// acme.sellers.v1.SellerScorecardSyncService/ResolveSellerScorecardConflict.
func (s *SellerPayoutScheduleApprovalServer) resolveSellerScorecardConflictSender(request *sellersv1.ResolveSellerScorecardConflictRequest) func(context.Context) (*sellersv1.ResolveSellerScorecardConflictResponse, error) {
	return func(ctx context.Context) (*sellersv1.ResolveSellerScorecardConflictResponse, error) {
		return s.sellerScorecardSyncClient.ResolveSellerScorecardConflict(ctx, request)
	}
}

// sellerPayoutScheduleApprovalServerAdvanceSellerWorkflowSteps are the ways to run
// acme.sellers.v1.SellerWorkflowService/AdvanceSellerWorkflow, by name.
var sellerPayoutScheduleApprovalServerAdvanceSellerWorkflowSteps = map[string]func(*SellerPayoutScheduleApprovalServer, context.Context, *sellersv1.AdvanceSellerWorkflowRequest) (*sellersv1.AdvanceSellerWorkflowResponse, error){
	"default": (*SellerPayoutScheduleApprovalServer).advanceSellerWorkflowStep,
	"audited": (*SellerPayoutScheduleApprovalServer).advanceSellerWorkflowAuditedStep,
}

func (s *SellerPayoutScheduleApprovalServer) advanceSellerWorkflowStep(ctx context.Context, request *sellersv1.AdvanceSellerWorkflowRequest) (*sellersv1.AdvanceSellerWorkflowResponse, error) {
	return s.sellerWorkflowClient.AdvanceSellerWorkflow(ctx, request)
}

func (s *SellerPayoutScheduleApprovalServer) advanceSellerWorkflowAuditedStep(ctx context.Context, request *sellersv1.AdvanceSellerWorkflowRequest) (*sellersv1.AdvanceSellerWorkflowResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.sellers.v1.SellerWorkflowService/AdvanceSellerWorkflow")
	return s.sellerWorkflowClient.AdvanceSellerWorkflow(ctx, request)
}
