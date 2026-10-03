package payments

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// DisputeSyncServer implements `acme.payments.v1.DisputeSyncService`.
// Keeps disputes in step with the systems of record in other regions.
type DisputeSyncServer struct {
	*disputeSyncHandlers
}

// disputeSyncHandlers holds the handlers of DisputeSyncServer, with what they use.
type disputeSyncHandlers struct {
	paymentsv1.UnimplementedDisputeSyncServiceServer
	store                              *Store
	log                                *slog.Logger
	customerCalculationClient          customersv1.CustomerCalculationServiceClient
	previewPreferenceFn                func(context.Context, *customersv1.PreviewPreferenceRequest, ...grpc.CallOption) (*customersv1.PreviewPreferenceResponse, error)
	paymentLifecycleClient             paymentsv1.PaymentLifecycleServiceClient
	meteredLedgerAccount               meteredLedgerAccount
	paymentInstrumentReportingClient   paymentsv1.PaymentInstrumentReportingServiceClient
	conn                               *grpc.ClientConn
	userHistoryClient                  identityv1.UserHistoryServiceClient
	meteredCustomerSegmentValidation   meteredCustomerSegmentValidation
	stockItemWorkflowClient            inventoryv1.StockItemWorkflowServiceClient
	meteredCustomerValidation          meteredCustomerValidation
	restockDecisionReportingClient     returnsv1.RestockDecisionReportingServiceClient
	paymentClient                      paymentsv1.PaymentServiceClient
	meteredPaymentInstrumentValidation meteredPaymentInstrumentValidation
}

// NewDisputeSyncServer wires a DisputeSyncServer.
func NewDisputeSyncServer(deps platform.Deps) *DisputeSyncServer {
	preferenceValidation := customersv1.NewPreferenceValidationServiceClient(deps.Conn)
	return &DisputeSyncServer{disputeSyncHandlers: &disputeSyncHandlers{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.payments.v1.DisputeSyncService"),
		customerCalculationClient:          customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		previewPreferenceFn:                preferenceValidation.PreviewPreference,
		paymentLifecycleClient:             paymentsv1.NewPaymentLifecycleServiceClient(deps.Conn),
		meteredLedgerAccount:               meteredLedgerAccount{LedgerAccountServiceClient: ledgerv1.NewLedgerAccountServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		paymentInstrumentReportingClient:   paymentsv1.NewPaymentInstrumentReportingServiceClient(deps.Conn),
		conn:                               deps.Conn,
		userHistoryClient:                  identityv1.NewUserHistoryServiceClient(deps.Conn),
		meteredCustomerSegmentValidation:   meteredCustomerSegmentValidation{CustomerSegmentValidationServiceClient: customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		stockItemWorkflowClient:            inventoryv1.NewStockItemWorkflowServiceClient(deps.Conn),
		meteredCustomerValidation:          meteredCustomerValidation{CustomerValidationServiceClient: customersv1.NewCustomerValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		restockDecisionReportingClient:     returnsv1.NewRestockDecisionReportingServiceClient(deps.Conn),
		paymentClient:                      paymentsv1.NewPaymentServiceClient(deps.Conn),
		meteredPaymentInstrumentValidation: meteredPaymentInstrumentValidation{PaymentInstrumentValidationServiceClient: paymentsv1.NewPaymentInstrumentValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}}
}

// SyncDisputes syncs a dispute.
func (s *disputeSyncHandlers) SyncDisputes(ctx context.Context, req *paymentsv1.SyncDisputesRequest) (*paymentsv1.SyncDisputesResponse, error) {
	if err := s.store.SaveDispute(ctx, req.GetContext().GetTenantId(), req.GetDispute()); err != nil {
		return nil, grpcx.Wrap(err, "save dispute")
	}
	s.log.InfoContext(ctx, "sync disputes", "tenant", req.GetContext().GetTenantId(), "etag", req.GetDispute().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetDispute())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded paymentsv1.Dispute
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	estimateCustomerRequest := &customersv1.EstimateCustomerRequest{}
	estimateCustomerRequest.Context = req.GetContext()
	estimateCustomerRequest.Customer = &customersv1.Customer{
		Contact: &commonv1.ContactInfo{Email: (&decoded).GetRequester().GetEmail()},
	}
	estimateCustomerReply, err := s.customerCalculationClient.EstimateCustomer(ctx, estimateCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/EstimateCustomer")
	}
	previewPreferenceRequest := &customersv1.PreviewPreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{
				Etag: estimateCustomerReply.GetCustomer().GetAudit().GetEtag(),
			},
			Owner: &identityv1.UserRef{
				UserId:      estimateCustomerReply.GetCustomer().GetRelationshipManager().GetUserId(),
				DisplayName: estimateCustomerReply.GetCustomer().GetRelationshipManager().GetDisplayName(),
			},
		},
	}
	previewPreferenceReply, err := s.previewPreferenceFn(ctx, previewPreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/PreviewPreference")
	}
	schedulePaymentExpiryReply, err := s.paymentLifecycleClient.SchedulePaymentExpiry(ctx, &paymentsv1.SchedulePaymentExpiryRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: req.GetDispute().GetAssignee().GetDisplayName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentLifecycleService/SchedulePaymentExpiry")
	}
	return &paymentsv1.SyncDisputesResponse{
		Dispute: &paymentsv1.Dispute{
			Assignee: &identityv1.UserRef{
				Email: estimateCustomerReply.GetCustomer().GetContact().GetEmail(),
			},
			Requester: &commonv1.ContactInfo{
				Email: estimateCustomerReply.GetCustomer().GetContact().GetEmail(),
			},
			Sla:       &paymentsv1.DisputeSla{DueAt: schedulePaymentExpiryReply.GetPayment().GetDueAt()},
			DisputeId: req.GetDisputeId(),
			Subject:   req.GetDispute().GetSubject(),
		},
		Result: &commonv1.OperationResult{Status: previewPreferenceReply.GetOutcome().GetStatus()},
		Outcome: &paymentsv1.SyncDisputesResponse_Outcome{
			EffectiveAt: previewPreferenceReply.GetOutcome().GetEffectiveAt(),
		},
	}, nil
}

// PushDispute pushs a dispute.
func (s *disputeSyncHandlers) PushDispute(ctx context.Context, req *paymentsv1.PushDisputeRequest) (*paymentsv1.PushDisputeResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveDispute(ctx, req.GetContext().GetTenantId(), req.GetDispute()); err != nil {
		return nil, grpcx.Wrap(err, "save dispute")
	}
	s.log.InfoContext(ctx, "push dispute", "tenant", req.GetContext().GetTenantId(), "etag", req.GetDispute().GetAudit().GetEtag())
	searchLedgerAccountsRequest := &ledgerv1.SearchLedgerAccountsRequest{
		Context: req.GetContext(),
		Filter: &ledgerv1.SearchLedgerAccountsRequest_Filter{
			CreatedAfter: req.GetDispute().GetSla().GetDueAt(),
			OwnerId:      req.GetDispute().GetSla().GetPolicyId(),
			Labels:       req.GetAnnotations(),
		},
	}
	searchLedgerAccountsReply, err := s.meteredLedgerAccount.SearchLedgerAccounts(ctx, searchLedgerAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/SearchLedgerAccounts")
	}
	countPaymentInstrumentTotalsRequest := &paymentsv1.CountPaymentInstrumentTotalsRequest{}
	countPaymentInstrumentTotalsRequest.Context = req.GetContext()
	countPaymentInstrumentTotalsRequest.Filter = &paymentsv1.CountPaymentInstrumentTotalsRequest_Filter{
		CreatedAfter: req.GetDispute().GetSla().GetDueAt(),
		OwnerId:      req.GetDispute().GetSla().GetPolicyId(),
	}
	countPaymentInstrumentTotalsReply, err := s.paymentInstrumentReportingClient.CountPaymentInstrumentTotals(ctx, countPaymentInstrumentTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentReportingService/CountPaymentInstrumentTotals")
	}
	_ = countPaymentInstrumentTotalsReply
	return &paymentsv1.PushDisputeResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: searchLedgerAccountsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Dispute: &paymentsv1.Dispute{
			DisputeId: req.GetDisputeId(),
			Assignee:  req.GetDispute().GetAssignee(),
		},
	}, nil
}

// PullDisputes pulls a dispute.
func (s *disputeSyncHandlers) PullDisputes(ctx context.Context, req *paymentsv1.PullDisputesRequest) (*paymentsv1.PullDisputesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveDispute(ctx, req.GetContext().GetTenantId(), req.GetDispute()); err != nil {
		return nil, grpcx.Wrap(err, "save dispute")
	}
	s.log.InfoContext(ctx, "pull disputes", "tenant", req.GetContext().GetTenantId(), "etag", req.GetDispute().GetAudit().GetEtag())
	var number string
	switch choice := req.GetDispute().GetOrigin().(type) {
	case *paymentsv1.Dispute_Customer:
		number = choice.Customer.GetNumber()
	case *paymentsv1.Dispute_ExternalReference:
		number = choice.ExternalReference
	case *paymentsv1.Dispute_Channel:
		number = choice.Channel.GetChannel()
	}
	snapshot, err := protojson.Marshal(req.GetDispute())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pullPayoutsRequest := &paymentsv1.PullPayoutsRequest{
		Context: req.GetContext(),
		Payout: &paymentsv1.Payout{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetDispute().GetAudit().GetUpdatedBy()},
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: req.GetDispute().GetAssignee().GetDisplayName(),
				Number:      number,
			},
		},
		Details: &paymentsv1.PullPayoutsRequest_Details{Payload: snapshot},
	}
	pullPayoutsReply := new(paymentsv1.PullPayoutsResponse)
	if err := grpcx.Invoke(ctx, s.conn, paymentsv1.PayoutSyncService_PullPayouts_FullMethodName, pullPayoutsRequest, pullPayoutsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutSyncService/PullPayouts")
	}
	recordUserVersionReply, err := s.userHistoryClient.RecordUserVersion(ctx, &identityv1.RecordUserVersionRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			Audit: &commonv1.AuditInfo{
				CreatedBy: pullPayoutsReply.GetPayout().GetAudit().GetCreatedBy(),
			},
			PrimaryAddress: &commonv1.PostalAddress{
				CountryCode: pullPayoutsReply.GetPayout().GetBillingAddress().GetCountryCode(),
			},
			RelationshipManager: &identityv1.UserRef{Email: req.GetDispute().GetRequester().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserHistoryService/RecordUserVersion")
	}
	return &paymentsv1.PullDisputesResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: pullPayoutsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Dispute: &paymentsv1.Dispute{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: pullPayoutsReply.GetPayout().GetAudit().GetUpdatedAt(),
			},
			Requester: &commonv1.ContactInfo{
				Name: recordUserVersionReply.GetUser().GetContact().GetName(),
			},
			DisputeId: req.GetDisputeId(),
			Subject:   req.GetDispute().GetSubject(),
		},
		Outcome: &paymentsv1.PullDisputesResponse_Outcome{
			EffectiveAt: recordUserVersionReply.GetOutcome().GetEffectiveAt(),
		},
	}, nil
}

// ReconcileDisputes reconciles a dispute.
func (s *disputeSyncHandlers) ReconcileDisputes(ctx context.Context, req *paymentsv1.ReconcileDisputesRequest) (*paymentsv1.ReconcileDisputesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveDispute(ctx, req.GetContext().GetTenantId(), req.GetDispute()); err != nil {
		return nil, grpcx.Wrap(err, "save dispute")
	}
	s.log.InfoContext(ctx, "reconcile disputes", "tenant", req.GetContext().GetTenantId(), "etag", req.GetDispute().GetAudit().GetEtag())
	explainCustomerSegmentDecisionRequest := &customersv1.ExplainCustomerSegmentDecisionRequest{
		Context: req.GetContext(),
		Details: &customersv1.ExplainCustomerSegmentDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDispute().GetRequester().GetName()},
		},
	}
	explainCustomerSegmentDecisionReply, err := s.meteredCustomerSegmentValidation.ExplainCustomerSegmentDecision(ctx, explainCustomerSegmentDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/ExplainCustomerSegmentDecision")
	}
	return &paymentsv1.ReconcileDisputesResponse{
		Dispute: &paymentsv1.Dispute{
			Assignee: &identityv1.UserRef{
				UserId: explainCustomerSegmentDecisionReply.GetCustomerSegment().GetOwner().GetUserId(),
			},
			DisputeId: req.GetDisputeId(),
			Subject:   req.GetDispute().GetSubject(),
		},
		Result: &commonv1.OperationResult{
			Details: explainCustomerSegmentDecisionReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// StreamDisputeChanges exchanges changes of disputes with a peer.
func (s *disputeSyncHandlers) StreamDisputeChanges(stream grpc.BidiStreamingServer[paymentsv1.StreamDisputeChangesRequest, paymentsv1.StreamDisputeChangesResponse]) error {
	ctx := stream.Context()
	incoming := make(chan *paymentsv1.StreamDisputeChangesRequest)
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
		s.log.InfoContext(ctx, "stream dispute changes", "tenant", msg.GetContext().GetTenantId())
		advanceStockItemWorkflowRequest := &inventoryv1.AdvanceStockItemWorkflowRequest{
			Context: msg.GetContext(),
			Details: &inventoryv1.AdvanceStockItemWorkflowRequest_Details{
				Notify: &commonv1.ContactInfo{
					Phone:            msg.GetChange().GetRequester().GetPhone(),
					PreferredChannel: msg.GetChange().GetRequester().GetPreferredChannel(),
				},
			},
			StockItem: &inventoryv1.StockItem{
				Audit: &commonv1.AuditInfo{
					UpdatedBy: msg.GetChange().GetAudit().GetUpdatedBy(),
					CreatedBy: msg.GetChange().GetAudit().GetCreatedBy(),
				},
			},
		}
		if _, err := grpcx.Call(ctx, s.stockItemWorkflowClient.AdvanceStockItemWorkflow, advanceStockItemWorkflowRequest); err != nil {
			return grpcx.Wrap(err, "acme.inventory.v1.StockItemWorkflowService/AdvanceStockItemWorkflow")
		}
		lintCustomersRequest := &customersv1.LintCustomersRequest{
			Context: msg.GetContext(),
			Customer: &customersv1.Customer{
				Audit:   &commonv1.AuditInfo{Etag: msg.GetChange().GetAudit().GetEtag()},
				Contact: &commonv1.ContactInfo{Email: msg.GetChange().GetAssignee().GetEmail()},
			},
		}
		if _, err := s.meteredCustomerValidation.LintCustomers(ctx, lintCustomersRequest); err != nil {
			return grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/LintCustomers")
		}
		if err := stream.Send(&paymentsv1.StreamDisputeChangesResponse{Result: &commonv1.OperationResult{Status: "ok"}}); err != nil {
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

// ResolveDisputeConflict resolves a dispute.
func (s *disputeSyncHandlers) ResolveDisputeConflict(ctx context.Context, req *paymentsv1.ResolveDisputeConflictRequest) (*paymentsv1.ResolveDisputeConflictResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ResolveDisputeConflict", "error", err)
		return nil, err
	}
	if err := s.store.SaveDispute(ctx, req.GetContext().GetTenantId(), req.GetDispute()); err != nil {
		return nil, grpcx.Wrap(err, "save dispute")
	}
	s.log.InfoContext(ctx, "resolve dispute conflict", "tenant", req.GetContext().GetTenantId(), "etag", req.GetDispute().GetAudit().GetEtag())
	listRestockDecisionReportsRequest := &returnsv1.ListRestockDecisionReportsRequest{}
	listRestockDecisionReportsRequest.Context = req.GetContext()
	listRestockDecisionReportsRequest.Filter = &returnsv1.ListRestockDecisionReportsRequest_Filter{
		CreatedAfter: req.GetDispute().GetAudit().GetCreatedAt(),
		OwnerId:      req.GetDispute().GetSla().GetPolicyId(),
	}
	listRestockDecisionReportsReply, err := s.restockDecisionReportingClient.ListRestockDecisionReports(ctx, listRestockDecisionReportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionReportingService/ListRestockDecisionReports")
	}
	createPaymentReply, err := s.paymentClient.CreatePayment(ctx, &paymentsv1.CreatePaymentRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetDispute().GetAudit().GetEtag(),
				UpdatedAt: req.GetDispute().GetAudit().GetUpdatedAt(),
				CreatedAt: req.GetDispute().GetAudit().GetCreatedAt(),
			},
			Attachments: []*paymentsv1.PaymentAttachment{&paymentsv1.PaymentAttachment{Checksum: req.GetDetails().GetChecksum()}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentService/CreatePayment")
	}
	return &paymentsv1.ResolveDisputeConflictResponse{
		Result: &commonv1.OperationResult{
			Status: listRestockDecisionReportsReply.GetResult().GetStatus(),
		},
		Outcome: &paymentsv1.ResolveDisputeConflictResponse_Outcome{
			Message: listRestockDecisionReportsReply.GetResult().GetMessage(),
		},
		Dispute: &paymentsv1.Dispute{
			Audit: &commonv1.AuditInfo{
				CreatedBy: createPaymentReply.GetPayment().GetAudit().GetCreatedBy(),
			},
			DisputeId: req.GetDisputeId(),
			Subject:   req.GetDispute().GetSubject(),
		},
	}, nil
}

// GetDisputeSyncState returns a dispute.
func (s *disputeSyncHandlers) GetDisputeSyncState(ctx context.Context, req *paymentsv1.GetDisputeSyncStateRequest) (*paymentsv1.GetDisputeSyncStateResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetDisputeSyncState", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "dispute", req.GetDisputeId(), "get_dispute_sync_state"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get dispute sync state", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	shareRefundReportRequest := &paymentsv1.ShareRefundReportRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.ShareRefundReportRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Payload:   packed,
		},
	}
	shareRefundReportReply := new(paymentsv1.ShareRefundReportResponse)
	if err := s.conn.Invoke(ctx, paymentsRefundReportingServiceShareRefundReportMethod, shareRefundReportRequest, shareRefundReportReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundReportingService/ShareRefundReport")
	}
	scorePaymentInstrumentRequest := &paymentsv1.ScorePaymentInstrumentRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Audit: &commonv1.AuditInfo{
				CreatedAt: shareRefundReportReply.GetRefund().GetAudit().GetCreatedAt(),
				Etag:      shareRefundReportReply.GetRefund().GetAudit().GetEtag(),
			},
			Funding: &paymentsv1.PaymentInstrument_Card{Card: &paymentsv1.PaymentInstrumentCardFunding{
				HolderName: shareRefundReportReply.GetRefund().GetCard().GetHolderName(),
			}},
		},
		Details: &paymentsv1.ScorePaymentInstrumentRequest_Details{
			Destination: &commonv1.PostalAddress{
				Lines: shareRefundReportReply.GetRefund().GetBillingAddress().GetLines(),
			},
		},
	}
	scorePaymentInstrumentReply, err := s.meteredPaymentInstrumentValidation.ScorePaymentInstrument(ctx, scorePaymentInstrumentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentValidationService/ScorePaymentInstrument")
	}
	return &paymentsv1.GetDisputeSyncStateResponse{
		Result: &commonv1.OperationResult{
			Message: shareRefundReportReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Dispute: &paymentsv1.Dispute{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: scorePaymentInstrumentReply.GetPaymentInstrument().GetAudit().GetUpdatedBy(),
			},
			DisputeId: req.GetDisputeId(),
		},
	}, nil
}

// ListDisputeConflicts lists disputes a page at a time.
func (s *disputeSyncHandlers) ListDisputeConflicts(ctx context.Context, req *paymentsv1.ListDisputeConflictsRequest) (*paymentsv1.ListDisputeConflictsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list dispute conflicts", "tenant", req.GetContext().GetTenantId())
	return &paymentsv1.ListDisputeConflictsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}
