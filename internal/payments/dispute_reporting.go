package payments

import (
	"context"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	messagingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/messaging/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// DisputeReportingServer implements `acme.payments.v1.DisputeReportingService`.
// Builds and schedules reports about disputes.
type DisputeReportingServer struct {
	paymentsv1.UnimplementedDisputeReportingServiceServer
	store                              *Store
	log                                *slog.Logger
	customerCalculationClient          customersv1.CustomerCalculationServiceClient
	conn                               *grpc.ClientConn
	parcelWorkflowClient               shippingv1.ParcelWorkflowServiceClient
	bus                                *eventbus.Bus
	clients                            *registry.Registry
	meteredAuditEventQuery             meteredAuditEventQuery
	giftCardTransactionLifecycleClient giftcardsv1.GiftCardTransactionLifecycleServiceClient
	trackingEventLifecycleClient       shippingv1.TrackingEventLifecycleServiceClient
	paymentInstrumentSyncClient        paymentsv1.PaymentInstrumentSyncServiceClient
	ledgerAccountOnce                  sync.Once
	ledgerAccountCached                ledgerv1.LedgerAccountServiceClient
	disputeImportClient                paymentsv1.DisputeImportServiceClient
	disputeImportClientFallback        paymentsv1.DisputeImportServiceClient
}

// NewDisputeReportingServer wires a DisputeReportingServer.
func NewDisputeReportingServer(deps platform.Deps) *DisputeReportingServer {
	return &DisputeReportingServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.payments.v1.DisputeReportingService"),
		customerCalculationClient:          customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		conn:                               deps.Conn,
		parcelWorkflowClient:               shippingv1.NewParcelWorkflowServiceClient(deps.Conn),
		bus:                                deps.Bus,
		clients:                            deps.Clients,
		meteredAuditEventQuery:             meteredAuditEventQuery{AuditEventQueryServiceClient: auditv1.NewAuditEventQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		giftCardTransactionLifecycleClient: giftcardsv1.NewGiftCardTransactionLifecycleServiceClient(deps.Conn),
		trackingEventLifecycleClient:       shippingv1.NewTrackingEventLifecycleServiceClient(deps.Conn),
		paymentInstrumentSyncClient:        paymentsv1.NewPaymentInstrumentSyncServiceClient(deps.Conn),
		disputeImportClient:                paymentsv1.NewDisputeImportServiceClient(deps.Conn),
		disputeImportClientFallback:        paymentsv1.NewDisputeImportServiceClient(deps.Conn),
	}
}

// GenerateDisputeReport generates a dispute.
func (s *DisputeReportingServer) GenerateDisputeReport(ctx context.Context, req *paymentsv1.GenerateDisputeReportRequest) (*paymentsv1.GenerateDisputeReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "dispute", req.GetDisputeId(), "generate_dispute_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate dispute report", "tenant", req.GetContext().GetTenantId())
	estimateCustomerRequest := &customersv1.EstimateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	estimateCustomerReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*customersv1.EstimateCustomerResponse, error) {
		return s.customerCalculationClient.EstimateCustomer(ctx, estimateCustomerRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/EstimateCustomer")
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	resolvePayoutConflictRequest := &paymentsv1.ResolvePayoutConflictRequest{
		Context: req.GetContext(),
		Payout: &paymentsv1.Payout{
			BillingAddress: &commonv1.PostalAddress{
				Recipient:   estimateCustomerReply.GetCustomer().GetPrimaryAddress().GetRecipient(),
				CountryCode: estimateCustomerReply.GetCustomer().GetPrimaryAddress().GetCountryCode(),
			},
			Counterparty: &paymentsv1.RefundRef{
				Number: estimateCustomerReply.GetCustomer().GetNationalId().GetNumber(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: estimateCustomerReply.GetCustomer().GetAudit().GetEtag(),
			},
		},
		Details: &paymentsv1.ResolvePayoutConflictRequest_Details{Payload: snapshot},
	}
	resolvePayoutConflictReply, err := grpcx.NewClient(s.conn, paymentsv1.NewPayoutSyncServiceClient).ResolvePayoutConflict(ctx, resolvePayoutConflictRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutSyncService/ResolvePayoutConflict")
	}
	return &paymentsv1.GenerateDisputeReportResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: estimateCustomerReply.GetResult().GetCorrelationId(),
			Message:       resolvePayoutConflictReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchDisputeReport returns a dispute.
func (s *DisputeReportingServer) FetchDisputeReport(ctx context.Context, req *paymentsv1.FetchDisputeReportRequest) (*paymentsv1.FetchDisputeReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected FetchDisputeReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "dispute", req.GetDisputeId(), "fetch_dispute_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch dispute report", "tenant", req.GetContext().GetTenantId())
	return &paymentsv1.FetchDisputeReportResponse{
		Dispute: &paymentsv1.Dispute{DisputeId: req.GetDisputeId()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScheduleDisputeReport schedules a dispute.
func (s *DisputeReportingServer) ScheduleDisputeReport(ctx context.Context, req *paymentsv1.ScheduleDisputeReportRequest) (*paymentsv1.ScheduleDisputeReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "dispute", req.GetDisputeId(), "schedule_dispute_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule dispute report", "tenant", req.GetContext().GetTenantId())
	listParcelWorkflowsRequest := buildListParcelWorkflowsRequestForScheduleDisputeReport(req)
	listParcelWorkflowsReply, err := s.parcelWorkflowClient.ListParcelWorkflows(ctx, listParcelWorkflowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelWorkflowService/ListParcelWorkflows")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "payments.schedule_dispute_report.list_payment_instrument_conflicts",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"filter.owner_id":   listParcelWorkflowsReply.GetResult().GetCorrelationId(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &paymentsv1.ScheduleDisputeReportResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  listParcelWorkflowsReply.GetResult().GetReferenceIds(),
			CorrelationId: listParcelWorkflowsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// StreamDisputeMetrics streams the changes of disputes as they happen.
func (s *DisputeReportingServer) StreamDisputeMetrics(req *paymentsv1.StreamDisputeMetricsRequest, stream grpc.ServerStreamingServer[paymentsv1.StreamDisputeMetricsResponse]) error {
	ctx := stream.Context()
	s.log.InfoContext(ctx, "stream dispute metrics", "tenant", req.GetContext().GetTenantId())
	customerValidation, err := registry.Lookup[customersv1.CustomerValidationServiceClient](s.clients, "acme.customers.v1.CustomerValidationService")
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	encoded, err := protojson.Marshal(req.GetSubscriber())
	if err != nil {
		return grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.ContactInfo
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return err
	}
	explainCustomerDecisionRequest := &customersv1.ExplainCustomerDecisionRequest{
		Context: req.GetContext(),
		Details: &customersv1.ExplainCustomerDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:  (&decoded).GetName(),
				Phone: req.GetSubscriber().GetPhone(),
			},
		},
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{Email: req.GetSubscriber().GetEmail()},
		},
	}
	explainCustomerDecisionReply, err := customerValidation.ExplainCustomerDecision(ctx, explainCustomerDecisionRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/ExplainCustomerDecision")
	}
	startStorageBinWorkflowRequest := &warehousingv1.StartStorageBinWorkflowRequest{
		Context: req.GetContext(),
		StorageBin: &warehousingv1.StorageBin{
			Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
				Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
					Location: &commonv1.PostalAddress{
						Locality:    explainCustomerDecisionReply.GetCustomer().GetPrimaryAddress().GetLocality(),
						Region:      explainCustomerDecisionReply.GetCustomer().GetPrimaryAddress().GetRegion(),
						CountryCode: explainCustomerDecisionReply.GetCustomer().GetPrimaryAddress().GetCountryCode(),
					},
					Audit: &commonv1.AuditInfo{
						CreatedBy: explainCustomerDecisionReply.GetCustomer().GetAudit().GetCreatedBy(),
					},
				}},
			}},
		},
	}
	startStorageBinWorkflowReply := new(warehousingv1.StartStorageBinWorkflowResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.warehousing.v1.StorageBinWorkflowService/StartStorageBinWorkflow", startStorageBinWorkflowRequest, startStorageBinWorkflowReply); err != nil {
		return grpcx.Wrap(err, "acme.warehousing.v1.StorageBinWorkflowService/StartStorageBinWorkflow")
	}
	suggestAuditEventsRequest := &auditv1.SuggestAuditEventsRequest{
		Context: req.GetContext(),
		Filter: &auditv1.SuggestAuditEventsRequest_Filter{
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			Region:       explainCustomerDecisionReply.GetCustomer().GetPrimaryAddress().GetRegion(),
		},
	}
	suggestAuditEventsReply, err := s.meteredAuditEventQuery.SuggestAuditEvents(ctx, suggestAuditEventsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.audit.v1.AuditEventQueryService/SuggestAuditEvents")
	}
	_ = suggestAuditEventsReply
	if err := stream.Send(&paymentsv1.StreamDisputeMetricsResponse{
		Result: &commonv1.OperationResult{
			Status:  explainCustomerDecisionReply.GetOutcome().GetStatus(),
			Message: explainCustomerDecisionReply.GetResult().GetMessage(),
		},
		Dispute: &paymentsv1.Dispute{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: startStorageBinWorkflowReply.GetStorageBin().GetAudit().GetUpdatedBy(),
			},
			Assignee: &identityv1.UserRef{
				UserId: startStorageBinWorkflowReply.GetStorageBin().GetManager().GetUserId(),
				Email:  req.GetSubscriber().GetEmail(),
			},
			Requester: &commonv1.ContactInfo{Name: req.GetSubscriber().GetName()},
		},
	}); err != nil {
		return err
	}
	return nil
}

// ListDisputeReports lists disputes a page at a time.
func (s *DisputeReportingServer) ListDisputeReports(ctx context.Context, req *paymentsv1.ListDisputeReportsRequest) (*paymentsv1.ListDisputeReportsResponse, error) {
	s.log.InfoContext(ctx, "list dispute reports", "tenant", req.GetContext().GetTenantId())
	source := req.GetFilter().ProtoReflect()
	ownerId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("owner_id"))).String()
	scheduleGiftCardTransactionExpiryRequest := &giftcardsv1.ScheduleGiftCardTransactionExpiryRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.ScheduleGiftCardTransactionExpiryRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Measurements: []*giftcardsv1.GiftCardTransactionMeasurement{&giftcardsv1.GiftCardTransactionMeasurement{ObservedAt: req.GetFilter().GetCreatedAfter()}},
			Source:       ownerId,
		},
	}
	type scheduleGiftCardTransactionExpiryResult struct {
		reply *giftcardsv1.ScheduleGiftCardTransactionExpiryResponse
		err   error
	}
	scheduleGiftCardTransactionExpiryResults := make(chan scheduleGiftCardTransactionExpiryResult, 1)
	go func() {
		reply, err := s.giftCardTransactionLifecycleClient.ScheduleGiftCardTransactionExpiry(ctx, scheduleGiftCardTransactionExpiryRequest)
		scheduleGiftCardTransactionExpiryResults <- scheduleGiftCardTransactionExpiryResult{reply, err}
	}()
	var scheduleGiftCardTransactionExpiryReply *giftcardsv1.ScheduleGiftCardTransactionExpiryResponse
	select {
	case result := <-scheduleGiftCardTransactionExpiryResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.giftcards.v1.GiftCardTransactionLifecycleService/ScheduleGiftCardTransactionExpiry")
		}
		scheduleGiftCardTransactionExpiryReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &paymentsv1.ListDisputeReportsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: scheduleGiftCardTransactionExpiryReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// ShareDisputeReport shares a dispute.
func (s *DisputeReportingServer) ShareDisputeReport(ctx context.Context, req *paymentsv1.ShareDisputeReportRequest) (*paymentsv1.ShareDisputeReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "dispute", req.GetDisputeId(), "share_dispute_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share dispute report", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	scheduleTrackingEventExpiryReply, err := s.trackingEventLifecycleClient.ScheduleTrackingEventExpiry(ctx, &shippingv1.ScheduleTrackingEventExpiryRequest{
		Context: req.GetContext(),
		Details: &shippingv1.ScheduleTrackingEventExpiryRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: req.GetDetails().GetRecipient().GetEmail(),
				Name:  req.GetDetails().GetRecipient().GetName(),
			},
		},
		TrackingEvent: &shippingv1.TrackingEvent{Detail: packed},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventLifecycleService/ScheduleTrackingEventExpiry")
	}
	return &paymentsv1.ShareDisputeReportResponse{
		Outcome: &paymentsv1.ShareDisputeReportResponse_Outcome{
			Message:     scheduleTrackingEventExpiryReply.GetResult().GetMessage(),
			ReferenceId: scheduleTrackingEventExpiryReply.GetOutcome().GetReferenceId(),
		},
		Dispute: &paymentsv1.Dispute{
			DisputeId: req.GetDisputeId(),
			Assignee:  &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CountDisputeTotals counts disputes by status.
func (s *DisputeReportingServer) CountDisputeTotals(ctx context.Context, req *paymentsv1.CountDisputeTotalsRequest) (*paymentsv1.CountDisputeTotalsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CountDisputeTotals", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "count dispute totals", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetFilter())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded paymentsv1.CountDisputeTotalsRequest_Filter
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	syncPaymentInstrumentsReply, err := s.paymentInstrumentSyncClient.SyncPaymentInstruments(ctx, &paymentsv1.SyncPaymentInstrumentsRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Labels:  req.GetFilter().GetLabels(),
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Audit:   &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
			Funding: &paymentsv1.PaymentInstrument_ExternalAccountId{ExternalAccountId: (&decoded).GetOwnerId()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentSyncService/SyncPaymentInstruments")
	}
	return &paymentsv1.CountDisputeTotalsResponse{
		Result: &commonv1.OperationResult{
			Message: syncPaymentInstrumentsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// DeleteDisputeReport deletes a dispute.
func (s *DisputeReportingServer) DeleteDisputeReport(ctx context.Context, req *paymentsv1.DeleteDisputeReportRequest) (*paymentsv1.DeleteDisputeReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "dispute", req.GetDisputeId(), "delete_dispute_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete dispute report", "tenant", req.GetContext().GetTenantId())
	cloneLedgerAccountRequest := &ledgerv1.CloneLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetEtag(),
				UpdatedAt: req.GetContext().GetDeadline(),
			},
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Funding: &ledgerv1.LedgerAccount_ExternalAccountId{ExternalAccountId: req.GetDisputeId()},
		},
	}
	cloneLedgerAccountReply, err := s.ledgerAccountLazy().CloneLedgerAccount(ctx, cloneLedgerAccountRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/CloneLedgerAccount")
	}
	routes := map[string]func(context.Context, *paymentsv1.MapDisputeImportColumnsRequest, ...grpc.CallOption) (*paymentsv1.MapDisputeImportColumnsResponse, error){
		"primary":  s.disputeImportClient.MapDisputeImportColumns,
		"fallback": s.disputeImportClientFallback.MapDisputeImportColumns,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	mapDisputeImportColumnsRequest := &paymentsv1.MapDisputeImportColumnsRequest{
		Context: req.GetContext(),
		Dispute: &paymentsv1.Dispute{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: cloneLedgerAccountReply.GetLedgerAccount().GetAudit().GetUpdatedAt(),
				Etag:      cloneLedgerAccountReply.GetLedgerAccount().GetAudit().GetEtag(),
			},
			Requester: &commonv1.ContactInfo{
				Phone: cloneLedgerAccountReply.GetLedgerAccount().GetContact().GetPhone(),
			},
			Origin: &paymentsv1.Dispute_Customer{Customer: &paymentsv1.PaymentRef{
				DisplayName: cloneLedgerAccountReply.GetLedgerAccount().GetHolder().GetDisplayName(),
			}},
		},
	}
	mapDisputeImportColumnsReply, err := route(ctx, mapDisputeImportColumnsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeImportService/MapDisputeImportColumns")
	}
	createConversationRequest := &messagingv1.CreateConversationRequest{
		Context: req.GetContext(),
		Conversation: &messagingv1.Conversation{
			Comments: []*messagingv1.ConversationComment{&messagingv1.ConversationComment{
				CreatedAt: mapDisputeImportColumnsReply.GetDispute().GetAudit().GetCreatedAt(),
			}},
			Requester: &commonv1.ContactInfo{
				Name: mapDisputeImportColumnsReply.GetDispute().GetRequester().GetName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: mapDisputeImportColumnsReply.GetDispute().GetAudit().GetCreatedBy(),
			},
			Sla: &messagingv1.ConversationSla{
				Breached: mapDisputeImportColumnsReply.GetDispute().GetSla().GetBreached(),
			},
		},
	}
	createConversationReply := new(messagingv1.CreateConversationResponse)
	if err := grpc.Invoke(ctx, "/acme.messaging.v1.ConversationService/CreateConversation", createConversationRequest, createConversationReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.messaging.v1.ConversationService/CreateConversation")
	}
	return &paymentsv1.DeleteDisputeReportResponse{
		Result: &commonv1.OperationResult{
			Status:        cloneLedgerAccountReply.GetResult().GetStatus(),
			CorrelationId: createConversationReply.GetResult().GetCorrelationId(),
			ReferenceIds:  createConversationReply.GetResult().GetReferenceIds(),
		},
		EffectiveAt: mapDisputeImportColumnsReply.GetOutcome().GetEffectiveAt(),
	}, nil
}

// ledgerAccountLazy dials acme.ledger.v1.LedgerAccountService the first time a handler needs it.
func (s *DisputeReportingServer) ledgerAccountLazy() ledgerv1.LedgerAccountServiceClient {
	s.ledgerAccountOnce.Do(func() { s.ledgerAccountCached = ledgerv1.NewLedgerAccountServiceClient(s.conn) })
	return s.ledgerAccountCached
}
