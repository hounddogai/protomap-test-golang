package payments

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// PayoutSyncServer implements `acme.payments.v1.PayoutSyncService`.
// Keeps payouts in step with the systems of record in other regions.
type PayoutSyncServer struct {
	paymentsv1.UnimplementedPayoutSyncServiceServer
	store                         *Store
	log                           *slog.Logger
	conn                          *grpc.ClientConn
	disputeImportClient           paymentsv1.DisputeImportServiceClient
	paymentCalculationClient      paymentsv1.PaymentCalculationServiceClient
	refundCalculationClient       paymentsv1.RefundCalculationServiceClient
	disputeValidationClient       paymentsv1.DisputeValidationServiceClient
	ledgerAccountValidationClient ledgerv1.LedgerAccountValidationServiceClient
	customerCalculationClient     customersv1.CustomerCalculationServiceClient
	paymentInstrumentSyncClient   paymentsv1.PaymentInstrumentSyncServiceClient
	refundReportingClient         paymentsv1.RefundReportingServiceClient
	refundQueryClient             paymentsv1.RefundQueryServiceClient
}

// NewPayoutSyncServer wires a PayoutSyncServer.
func NewPayoutSyncServer(deps platform.Deps) *PayoutSyncServer {
	return &PayoutSyncServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.payments.v1.PayoutSyncService"),
		conn:                          deps.Conn,
		disputeImportClient:           paymentsv1.NewDisputeImportServiceClient(deps.Conn),
		paymentCalculationClient:      paymentsv1.NewPaymentCalculationServiceClient(deps.Conn),
		refundCalculationClient:       paymentsv1.NewRefundCalculationServiceClient(deps.Conn),
		disputeValidationClient:       paymentsv1.NewDisputeValidationServiceClient(deps.Conn),
		ledgerAccountValidationClient: ledgerv1.NewLedgerAccountValidationServiceClient(deps.Conn),
		customerCalculationClient:     customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		paymentInstrumentSyncClient:   paymentsv1.NewPaymentInstrumentSyncServiceClient(deps.Conn),
		refundReportingClient:         paymentsv1.NewRefundReportingServiceClient(deps.Conn),
		refundQueryClient:             paymentsv1.NewRefundQueryServiceClient(deps.Conn),
	}
}

// SyncPayouts syncs a payout.
func (s *PayoutSyncServer) SyncPayouts(ctx context.Context, req *paymentsv1.SyncPayoutsRequest) (*paymentsv1.SyncPayoutsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePayout(ctx, req.GetContext().GetTenantId(), req.GetPayout()); err != nil {
		return nil, grpcx.Wrap(err, "save payout")
	}
	s.log.InfoContext(ctx, "sync payouts", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayout().GetAudit().GetEtag())
	listPaymentInstrumentConflictsRequest := &paymentsv1.ListPaymentInstrumentConflictsRequest{
		Context: req.GetContext(),
		Filter:  &paymentsv1.ListPaymentInstrumentConflictsRequest_Filter{Region: req.GetPayout().GetBillingAddress().GetRegion()},
	}
	listPaymentInstrumentConflictsReply := new(paymentsv1.ListPaymentInstrumentConflictsResponse)
	if err := s.conn.Invoke(ctx, paymentsPaymentInstrumentSyncServiceListPaymentInstrumentConflictsMethod, listPaymentInstrumentConflictsRequest, listPaymentInstrumentConflictsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentSyncService/ListPaymentInstrumentConflicts")
	}
	var holder struct{ Etag string }
	if record := req.GetPayout().GetAudit(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Etag"); err != nil {
			s.log.WarnContext(ctx, "copy etag", "error", err)
		}
	}
	validateDisputeImportRequest := &paymentsv1.ValidateDisputeImportRequest{
		Context: req.GetContext(),
		Dispute: &paymentsv1.Dispute{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetPayout().GetAudit().GetUpdatedBy(),
				Etag:      holder.Etag,
			},
			Comments: []*paymentsv1.DisputeComment{&paymentsv1.DisputeComment{CreatedAt: req.GetPayout().GetAudit().GetCreatedAt()}},
		},
	}
	validateDisputeImportReply, err := paymentsv1.DisputeImportServiceClient.ValidateDisputeImport(s.disputeImportClient, ctx, validateDisputeImportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeImportService/ValidateDisputeImport")
	}
	return &paymentsv1.SyncPayoutsResponse{
		Result: &commonv1.OperationResult{
			Details: listPaymentInstrumentConflictsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &paymentsv1.SyncPayoutsResponse_Outcome{
			Status:    listPaymentInstrumentConflictsReply.GetResult().GetStatus(),
			Message:   validateDisputeImportReply.GetResult().GetMessage(),
			NextSteps: validateDisputeImportReply.GetOutcome().GetNextSteps(),
		},
		Payout: &paymentsv1.Payout{
			PayoutId: req.GetPayoutId(),
			IssuedAt: req.GetPayout().GetIssuedAt(),
		},
	}, nil
}

// PushPayout pushs a payout.
func (s *PayoutSyncServer) PushPayout(ctx context.Context, req *paymentsv1.PushPayoutRequest) (*paymentsv1.PushPayoutResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePayout(ctx, req.GetContext().GetTenantId(), req.GetPayout()); err != nil {
		return nil, grpcx.Wrap(err, "save payout")
	}
	s.log.InfoContext(ctx, "push payout", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayout().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetPayout())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded paymentsv1.Payout
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	resumeRiskAssessmentWorkflowRequest := &fraudv1.ResumeRiskAssessmentWorkflowRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetPayout().GetAudit().GetEtag(),
				CreatedBy: (&decoded).GetAudit().GetCreatedBy(),
			},
			Origin: &fraudv1.RiskAssessment_Customer{Customer: &customersv1.CustomerRef{
				DisplayName: req.GetPayout().GetCounterparty().GetDisplayName(),
			}},
			Sla: &fraudv1.RiskAssessmentSla{DueAt: req.GetPayout().GetDueAt()},
		},
	}
	resumeRiskAssessmentWorkflowReply := new(fraudv1.ResumeRiskAssessmentWorkflowResponse)
	if err := grpcx.Invoke(ctx, s.conn, fraudv1.RiskAssessmentWorkflowService_ResumeRiskAssessmentWorkflow_FullMethodName, resumeRiskAssessmentWorkflowRequest, resumeRiskAssessmentWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentWorkflowService/ResumeRiskAssessmentWorkflow")
	}
	payoutCache := cache.New[string, *paymentsv1.Payout](1)
	payoutCache.Put(req.GetContext().GetTraceId(), req.GetPayout())
	cachedPayout, _ := payoutCache.Get(req.GetContext().GetTraceId())
	simulatePaymentRequest := &paymentsv1.SimulatePaymentRequest{}
	simulatePaymentRequest.Context = req.GetContext()
	simulatePaymentRequest.Payment = &paymentsv1.Payment{
		Audit: &commonv1.AuditInfo{
			CreatedAt: req.GetPayout().GetAudit().GetCreatedAt(),
			UpdatedBy: cachedPayout.GetAudit().GetUpdatedBy(),
		},
	}
	simulatePaymentReply, err := s.paymentCalculationClient.SimulatePayment(ctx, simulatePaymentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/SimulatePayment")
	}
	return &paymentsv1.PushPayoutResponse{
		Payout: &paymentsv1.Payout{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: resumeRiskAssessmentWorkflowReply.GetRiskAssessment().GetAudit().GetUpdatedAt(),
			},
			DueAt:    simulatePaymentReply.GetPayment().GetDueAt(),
			PayoutId: req.GetPayoutId(),
			Notes:    req.GetPayout().GetNotes(),
		},
		Outcome: &paymentsv1.PushPayoutResponse_Outcome{
			EffectiveAt: resumeRiskAssessmentWorkflowReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PullPayouts pulls a payout.
func (s *PayoutSyncServer) PullPayouts(ctx context.Context, req *paymentsv1.PullPayoutsRequest) (*paymentsv1.PullPayoutsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePayout(ctx, req.GetContext().GetTenantId(), req.GetPayout()); err != nil {
		return nil, grpcx.Wrap(err, "save payout")
	}
	s.log.InfoContext(ctx, "pull payouts", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayout().GetAudit().GetEtag())
	jobs := make(chan *paymentsv1.PayoutAttachment)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for attachment := range jobs {
				if _, err := s.refundCalculationClient.QuoteRefund(workCtx, &paymentsv1.QuoteRefundRequest{
					Context: req.GetContext(),
					Refund: &paymentsv1.Refund{
						Attachments: []*paymentsv1.RefundAttachment{&paymentsv1.RefundAttachment{
							Checksum: attachment.GetChecksum(),
							Uri:      attachment.GetUri(),
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
	for _, queued := range req.GetPayout().GetAttachments() {
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
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundCalculationService/QuoteRefund")
	}
	scoreDisputeRequest := &paymentsv1.ScoreDisputeRequest{
		Context: req.GetContext(),
		Dispute: &paymentsv1.Dispute{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetPayout().GetAudit().GetCreatedBy()},
		},
	}
	scoreDisputeReply, err := paymentsv1.DisputeValidationServiceClient.ScoreDispute(s.disputeValidationClient, ctx, scoreDisputeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeValidationService/ScoreDispute")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*ledgerv1.ScoreLedgerAccountResponse, len(req.GetPayout().GetLines()))
	for i, line := range req.GetPayout().GetLines() {
		group.Go(func() error {
			reply, err := s.ledgerAccountValidationClient.ScoreLedgerAccount(groupCtx, &ledgerv1.ScoreLedgerAccountRequest{
				Context: req.GetContext(),
				LedgerAccount: &ledgerv1.LedgerAccount{
					Limits: []*ledgerv1.LedgerAccountLimit{&ledgerv1.LedgerAccountLimit{Amount: line.GetTax().GetAmount()}},
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
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/ScoreLedgerAccount")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	mapDisputeImportColumnsReply, err := s.disputeImportClient.MapDisputeImportColumns(ctx, &paymentsv1.MapDisputeImportColumnsRequest{
		Context: req.GetContext(),
		Dispute: &paymentsv1.Dispute{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: scoreDisputeReply.GetDispute().GetAudit().GetUpdatedBy(),
			},
			Assignee: &identityv1.UserRef{
				Email: scoreDisputeReply.GetDispute().GetRequester().GetEmail(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeImportService/MapDisputeImportColumns")
	}
	return &paymentsv1.PullPayoutsResponse{
		Payout: &paymentsv1.Payout{
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: scoreDisputeReply.GetDispute().GetCustomer().GetDisplayName(),
			},
			PayoutId: req.GetPayoutId(),
			Number:   req.GetPayout().GetNumber(),
		},
		Outcome: &paymentsv1.PullPayoutsResponse_Outcome{
			EffectiveAt: mapDisputeImportColumnsReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Status: mapDisputeImportColumnsReply.GetResult().GetStatus(),
		},
	}, nil
}

// ReconcilePayouts reconciles a payout.
func (s *PayoutSyncServer) ReconcilePayouts(ctx context.Context, req *paymentsv1.ReconcilePayoutsRequest) (*paymentsv1.ReconcilePayoutsResponse, error) {
	if err := s.store.SavePayout(ctx, req.GetContext().GetTenantId(), req.GetPayout()); err != nil {
		return nil, grpcx.Wrap(err, "save payout")
	}
	s.log.InfoContext(ctx, "reconcile payouts", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayout().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*customersv1.RecalculateCustomersResponse, len(req.GetPayout().GetLines()))
	for i, line := range req.GetPayout().GetLines() {
		group.Go(func() error {
			reply, err := s.customerCalculationClient.RecalculateCustomers(groupCtx, &customersv1.RecalculateCustomersRequest{
				Context:  req.GetContext(),
				Customer: &customersv1.Customer{Attributes: line.GetAttributes()},
				Details: &customersv1.RecalculateCustomersRequest_Details{
					Destination: &commonv1.PostalAddress{
						CountryCode: req.GetPayout().GetBillingAddress().GetCountryCode(),
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
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/RecalculateCustomers")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &paymentsv1.ReconcilePayoutsResponse{
		Payout: &paymentsv1.Payout{
			PayoutId: req.GetPayoutId(),
			IssuedAt: req.GetPayout().GetIssuedAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// StreamPayoutChanges exchanges changes of payouts with a peer.
func (s *PayoutSyncServer) StreamPayoutChanges(stream grpc.BidiStreamingServer[paymentsv1.StreamPayoutChangesRequest, paymentsv1.StreamPayoutChangesResponse]) error {
	ctx := stream.Context()
	incoming := make(chan *paymentsv1.StreamPayoutChangesRequest)
	failed := make(chan error, 1)
	go func() {
		defer close(incoming)
		for {
			received, err := stream.Recv()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					failed <- err
				}
				return
			}
			select {
			case incoming <- received:
			case <-ctx.Done():
				return
			}
		}
	}()
	for msg := range incoming {
		s.log.InfoContext(ctx, "stream payout changes", "tenant", msg.GetContext().GetTenantId())
		if _, err := s.paymentCalculationClient.EstimatePayment(ctx, &paymentsv1.EstimatePaymentRequest{
			Context: msg.GetContext(),
			Payment: &paymentsv1.Payment{
				Settlement: &paymentsv1.Payment_Card{Card: &paymentsv1.PaymentCardSettlement{Network: msg.GetChange().GetCard().GetNetwork()}},
			},
		}); err != nil {
			return grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/EstimatePayment")
		}
		if _, err := s.paymentInstrumentSyncClient.ListPaymentInstrumentConflicts(ctx, &paymentsv1.ListPaymentInstrumentConflictsRequest{Context: msg.GetContext()}); err != nil {
			return grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentSyncService/ListPaymentInstrumentConflicts")
		}
		shareRefundReportRequest := &paymentsv1.ShareRefundReportRequest{
			Context: msg.GetContext(),
			Details: &paymentsv1.ShareRefundReportRequest_Details{
				Recipient: &commonv1.ContactInfo{Email: msg.GetContext().GetActor().GetEmail()},
			},
		}
		if _, err := grpcx.Call(ctx, s.refundReportingClient.ShareRefundReport, shareRefundReportRequest); err != nil {
			return grpcx.Wrap(err, "acme.payments.v1.RefundReportingService/ShareRefundReport")
		}
		if err := stream.Send(&paymentsv1.StreamPayoutChangesResponse{Result: &commonv1.OperationResult{Status: "ok"}}); err != nil {
			return err
		}
	}
	select {
	case err := <-failed:
		return err
	default:
		return nil
	}
}

// ResolvePayoutConflict resolves a payout.
func (s *PayoutSyncServer) ResolvePayoutConflict(ctx context.Context, req *paymentsv1.ResolvePayoutConflictRequest) (*paymentsv1.ResolvePayoutConflictResponse, error) {
	if err := s.store.SavePayout(ctx, req.GetContext().GetTenantId(), req.GetPayout()); err != nil {
		return nil, grpcx.Wrap(err, "save payout")
	}
	s.log.InfoContext(ctx, "resolve payout conflict", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayout().GetAudit().GetEtag())
	repairSynonymRequest := &searchv1.RepairSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetPayout().GetAudit().GetCreatedBy()},
		},
	}
	repairSynonymReply := new(searchv1.RepairSynonymResponse)
	if err := s.conn.Invoke(ctx, searchSynonymAdminServiceRepairSynonymMethod, repairSynonymRequest, repairSynonymReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymAdminService/RepairSynonym")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*paymentsv1.RecalculatePaymentsResponse, len(req.GetPayout().GetAttachments()))
	for i, attachment := range req.GetPayout().GetAttachments() {
		group.Go(func() error {
			reply, err := s.paymentCalculationClient.RecalculatePayments(groupCtx, &paymentsv1.RecalculatePaymentsRequest{
				Context: req.GetContext(),
				Payment: &paymentsv1.Payment{
					Attachments: []*paymentsv1.PaymentAttachment{&paymentsv1.PaymentAttachment{
						Checksum:  attachment.GetChecksum(),
						SizeBytes: attachment.GetSizeBytes(),
					}},
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
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/RecalculatePayments")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	findRefundsReply, err := s.refundQueryClient.FindRefunds(ctx, &paymentsv1.FindRefundsRequest{
		Context: req.GetContext(),
		Filter: &paymentsv1.FindRefundsRequest_Filter{
			CreatedAfter: repairSynonymReply.GetSynonym().GetAudit().GetUpdatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundQueryService/FindRefunds")
	}
	return &paymentsv1.ResolvePayoutConflictResponse{
		Outcome: &paymentsv1.ResolvePayoutConflictResponse_Outcome{Status: repairSynonymReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			Status:  findRefundsReply.GetResult().GetStatus(),
			Message: findRefundsReply.GetResult().GetMessage(),
		},
		Payout: &paymentsv1.Payout{
			PayoutId:     req.GetPayoutId(),
			Counterparty: &paymentsv1.RefundRef{Number: req.GetPayout().GetNumber()},
		},
	}, nil
}

// GetPayoutSyncState returns a payout.
func (s *PayoutSyncServer) GetPayoutSyncState(ctx context.Context, req *paymentsv1.GetPayoutSyncStateRequest) (*paymentsv1.GetPayoutSyncStateResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payout", req.GetPayoutId(), "get_payout_sync_state"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get payout sync state", "tenant", req.GetContext().GetTenantId())
	simulatePaymentReply, err := s.paymentCalculationClient.SimulatePayment(ctx, &paymentsv1.SimulatePaymentRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{DueAt: req.GetContext().GetDeadline()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/SimulatePayment")
	}
	return &paymentsv1.GetPayoutSyncStateResponse{
		Payout: &paymentsv1.Payout{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: simulatePaymentReply.GetPayment().GetBillingAddress().GetRecipient(),
			},
			PayoutId: req.GetPayoutId(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: simulatePaymentReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// ListPayoutConflicts lists payouts a page at a time.
func (s *PayoutSyncServer) ListPayoutConflicts(ctx context.Context, req *paymentsv1.ListPayoutConflictsRequest) (*paymentsv1.ListPayoutConflictsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list payout conflicts", "tenant", req.GetContext().GetTenantId())
	return &paymentsv1.ListPayoutConflictsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}
