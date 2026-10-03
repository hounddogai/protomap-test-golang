package payments

import (
	"context"
	"errors"
	"io"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PaymentInstrumentReportingServer implements `acme.payments.v1.PaymentInstrumentReportingService`.
// Builds and schedules reports about payment instruments.
type PaymentInstrumentReportingServer struct {
	paymentsv1.UnsafePaymentInstrumentReportingServiceServer
	store                          *Store
	log                            *slog.Logger
	clients                        *registry.Registry
	conn                           *grpc.ClientConn
	userHistoryClient              identityv1.UserHistoryServiceClient
	customerClient                 customersv1.CustomerServiceClient
	inspectionWorkflowClient       returnsv1.InspectionWorkflowServiceClient
	meteredPaymentValidation       meteredPaymentValidation
	searchIndexNotificationPort    searchIndexNotificationPort
	meteredSearchIndexNotification meteredSearchIndexNotification
	searchIndexNotificationClient  searchv1.SearchIndexNotificationServiceClient
	bus                            *eventbus.Bus
}

// NewPaymentInstrumentReportingServer wires a PaymentInstrumentReportingServer.
func NewPaymentInstrumentReportingServer(deps platform.Deps) *PaymentInstrumentReportingServer {
	return &PaymentInstrumentReportingServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.payments.v1.PaymentInstrumentReportingService"),
		clients:                        deps.Clients,
		conn:                           deps.Conn,
		userHistoryClient:              identityv1.NewUserHistoryServiceClient(deps.Conn),
		customerClient:                 customersv1.NewCustomerServiceClient(deps.Conn),
		inspectionWorkflowClient:       returnsv1.NewInspectionWorkflowServiceClient(deps.Conn),
		meteredPaymentValidation:       meteredPaymentValidation{PaymentValidationServiceClient: paymentsv1.NewPaymentValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		searchIndexNotificationPort:    newSearchIndexNotificationPort(deps),
		meteredSearchIndexNotification: meteredSearchIndexNotification{SearchIndexNotificationServiceClient: searchv1.NewSearchIndexNotificationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		searchIndexNotificationClient:  searchv1.NewSearchIndexNotificationServiceClient(deps.Conn),
		bus:                            deps.Bus,
	}
}

// GeneratePaymentInstrumentReport generates a payment instrument.
func (s *PaymentInstrumentReportingServer) GeneratePaymentInstrumentReport(ctx context.Context, req *paymentsv1.GeneratePaymentInstrumentReportRequest) (*paymentsv1.GeneratePaymentInstrumentReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment_instrument", req.GetPaymentInstrumentId(), "generate_payment_instrument_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate payment instrument report", "tenant", req.GetContext().GetTenantId())
	ledgerAccount, err := registry.Lookup[ledgerv1.LedgerAccountServiceClient](s.clients, "acme.ledger.v1.LedgerAccountService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	createLedgerAccountRequest := &ledgerv1.CreateLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Labels:  req.GetDetails().GetFilters(),
			Funding: &ledgerv1.LedgerAccount_ExternalAccountId{ExternalAccountId: req.GetPaymentInstrumentId()},
			Audit:   &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
		},
	}
	createLedgerAccountReply, err := ledgerAccount.CreateLedgerAccount(ctx, createLedgerAccountRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/CreateLedgerAccount")
	}
	countAuditEventsRequest := &auditv1.CountAuditEventsRequest{
		Context: req.GetContext(),
		Filter: &auditv1.CountAuditEventsRequest_Filter{
			Labels:       createLedgerAccountReply.GetLedgerAccount().GetLabels(),
			CreatedAfter: createLedgerAccountReply.GetLedgerAccount().GetOpenedAt(),
			OwnerId:      createLedgerAccountReply.GetLedgerAccount().GetHolder().GetPostingBatchId(),
		},
	}
	countAuditEventsReply := new(auditv1.CountAuditEventsResponse)
	if err := grpcx.Invoke(ctx, s.conn, auditv1.AuditEventQueryService_CountAuditEvents_FullMethodName, countAuditEventsRequest, countAuditEventsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventQueryService/CountAuditEvents")
	}
	return &paymentsv1.GeneratePaymentInstrumentReportResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: createLedgerAccountReply.GetResult().GetCorrelationId(),
			Details:       countAuditEventsReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchPaymentInstrumentReport returns a payment instrument.
func (s *PaymentInstrumentReportingServer) FetchPaymentInstrumentReport(ctx context.Context, req *paymentsv1.FetchPaymentInstrumentReportRequest) (*paymentsv1.FetchPaymentInstrumentReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected FetchPaymentInstrumentReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment_instrument", req.GetPaymentInstrumentId(), "fetch_payment_instrument_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch payment instrument report", "tenant", req.GetContext().GetTenantId())
	snapshot, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	recordUserVersionRequest := &identityv1.RecordUserVersionRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			RelationshipManager: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
		Details: &identityv1.RecordUserVersionRequest_Details{Snapshot: snapshot},
	}
	var recordUserVersionReply *identityv1.RecordUserVersionResponse
	if err := pipeline.Run(ctx, "acme.identity.v1.UserHistoryService/RecordUserVersion", func(ctx context.Context) error {
		answer, err := s.userHistoryClient.RecordUserVersion(ctx, recordUserVersionRequest)
		recordUserVersionReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserHistoryService/RecordUserVersion")
	}
	householdValidation, err := registry.Lookup[customersv1.HouseholdValidationServiceClient](s.clients, "acme.customers.v1.HouseholdValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	scoreHouseholdRequest := &customersv1.ScoreHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{
						Email: recordUserVersionReply.GetUser().GetRelationshipManager().GetEmail(),
					},
					Audit: &commonv1.AuditInfo{Etag: recordUserVersionReply.GetUser().GetAudit().GetEtag()},
					Location: &commonv1.PostalAddress{
						Recipient: recordUserVersionReply.GetUser().GetPrimaryAddress().GetRecipient(),
						Locality:  recordUserVersionReply.GetUser().GetPrimaryAddress().GetLocality(),
					},
				}},
			}},
		},
	}
	scoreHouseholdReply, err := householdValidation.ScoreHousehold(ctx, scoreHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/ScoreHousehold")
	}
	return &paymentsv1.FetchPaymentInstrumentReportResponse{
		Result: &commonv1.OperationResult{
			Details: recordUserVersionReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Limits: []*paymentsv1.PaymentInstrumentLimit{&paymentsv1.PaymentInstrumentLimit{Name: scoreHouseholdReply.GetHousehold().GetName()}},
			Contact: &commonv1.ContactInfo{
				Email: scoreHouseholdReply.GetHousehold().GetManager().GetEmail(),
			},
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
		},
	}, nil
}

// SchedulePaymentInstrumentReport schedules a payment instrument.
func (s *PaymentInstrumentReportingServer) SchedulePaymentInstrumentReport(ctx context.Context, req *paymentsv1.SchedulePaymentInstrumentReportRequest) (*paymentsv1.SchedulePaymentInstrumentReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment_instrument", req.GetPaymentInstrumentId(), "schedule_payment_instrument_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule payment instrument report", "tenant", req.GetContext().GetTenantId())
	customerSegmentCalculation, ok := s.clients.Get("acme.customers.v1.CustomerSegmentCalculationService").(customersv1.CustomerSegmentCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.customers.v1.CustomerSegmentCalculationService")
	}
	simulateCustomerSegmentRequest := &customersv1.SimulateCustomerSegmentRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Action: &customersv1.CustomerSegment_Block{Block: &customersv1.CustomerSegmentBlockAction{Reason: req.GetReason()}},
		},
	}
	simulateCustomerSegmentReply, err := customerSegmentCalculation.SimulateCustomerSegment(ctx, simulateCustomerSegmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/SimulateCustomerSegment")
	}
	validateDisputeImportRequest := &paymentsv1.ValidateDisputeImportRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.ValidateDisputeImportRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: simulateCustomerSegmentReply.GetCustomerSegment().GetOwner().GetEmail(),
			},
		},
		Dispute: &paymentsv1.Dispute{
			Audit: &commonv1.AuditInfo{
				CreatedAt: simulateCustomerSegmentReply.GetCustomerSegment().GetAudit().GetCreatedAt(),
			},
			Assignee: &identityv1.UserRef{
				DisplayName: simulateCustomerSegmentReply.GetCustomerSegment().GetOwner().GetDisplayName(),
				UserId:      simulateCustomerSegmentReply.GetCustomerSegment().GetOwner().GetUserId(),
			},
		},
	}
	validateDisputeImportReply, err := grpcx.NewClient(s.conn, paymentsv1.NewDisputeImportServiceClient).ValidateDisputeImport(ctx, validateDisputeImportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeImportService/ValidateDisputeImport")
	}
	return &paymentsv1.SchedulePaymentInstrumentReportResponse{
		Result: &commonv1.OperationResult{
			Status:  simulateCustomerSegmentReply.GetResult().GetStatus(),
			Message: simulateCustomerSegmentReply.GetResult().GetMessage(),
			Details: validateDisputeImportReply.GetResult().GetDetails(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// StreamPaymentInstrumentMetrics streams the changes of payment instruments as they happen.
func (s *PaymentInstrumentReportingServer) StreamPaymentInstrumentMetrics(req *paymentsv1.StreamPaymentInstrumentMetricsRequest, stream grpc.ServerStreamingServer[paymentsv1.StreamPaymentInstrumentMetricsResponse]) error {
	ctx := stream.Context()
	s.log.InfoContext(ctx, "stream payment instrument metrics", "tenant", req.GetContext().GetTenantId())
	var holder struct{ Query string }
	if record := req.GetFilter(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Query"); err != nil {
			s.log.WarnContext(ctx, "copy query", "error", err)
		}
	}
	searchLedgerAccountsRequest := &ledgerv1.SearchLedgerAccountsRequest{
		Context: req.GetContext(),
		Filter: &ledgerv1.SearchLedgerAccountsRequest_Filter{
			Labels:       req.GetFilter().GetLabels(),
			Region:       req.GetFilter().GetRegion(),
			Query:        holder.Query,
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
		},
	}
	searchLedgerAccountsReply := new(ledgerv1.SearchLedgerAccountsResponse)
	if err := grpc.Invoke(ctx, "/acme.ledger.v1.LedgerAccountService/SearchLedgerAccounts", searchLedgerAccountsRequest, searchLedgerAccountsReply, s.conn); err != nil {
		return grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/SearchLedgerAccounts")
	}
	updateCustomerReply, err := s.customerClient.UpdateCustomer(ctx, &customersv1.UpdateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Contact: &commonv1.ContactInfo{
				Name:             req.GetSubscriber().GetName(),
				PreferredChannel: req.GetSubscriber().GetPreferredChannel(),
			},
			PrimaryAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.customers.v1.CustomerService/UpdateCustomer")
	}
	signalInspectionWorkflowRequest := &returnsv1.SignalInspectionWorkflowRequest{}
	signalInspectionWorkflowRequest.Context = req.GetContext()
	signalInspectionWorkflowRequest.Inspection = &returnsv1.Inspection{
		Requester: &commonv1.ContactInfo{
			Email: updateCustomerReply.GetCustomer().GetContact().GetEmail(),
		},
	}
	signalInspectionWorkflowReply, err := s.inspectionWorkflowClient.SignalInspectionWorkflow(ctx, signalInspectionWorkflowRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.returns.v1.InspectionWorkflowService/SignalInspectionWorkflow")
	}
	validatePaymentRequest := &paymentsv1.ValidatePaymentRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Counterparty: &paymentsv1.RefundRef{
				Number: updateCustomerReply.GetCustomer().GetNationalId().GetNumber(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: signalInspectionWorkflowReply.GetInspection().GetAudit().GetEtag(),
			},
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: updateCustomerReply.GetCustomer().GetPrimaryAddress().GetCountryCode(),
			},
		},
		Details: &paymentsv1.ValidatePaymentRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: signalInspectionWorkflowReply.GetInspection().GetCustomer().GetEmail(),
			},
		},
	}
	validatePaymentReply, err := s.meteredPaymentValidation.ValidatePayment(ctx, validatePaymentRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.payments.v1.PaymentValidationService/ValidatePayment")
	}
	_ = validatePaymentReply
	if err := stream.Send(&paymentsv1.StreamPaymentInstrumentMetricsResponse{
		Result: &commonv1.OperationResult{Status: searchLedgerAccountsReply.GetResult().GetStatus()},
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Holder: &customersv1.CustomerRef{
				DisplayName: updateCustomerReply.GetCustomer().GetRelationshipManager().GetDisplayName(),
				Email:       signalInspectionWorkflowReply.GetInspection().GetAssignee().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: updateCustomerReply.GetCustomer().GetAudit().GetCreatedAt(),
			},
			Contact: &commonv1.ContactInfo{
				Name:  req.GetSubscriber().GetName(),
				Phone: req.GetSubscriber().GetPhone(),
			},
		},
	}); err != nil {
		return err
	}
	return nil
}

// SharePaymentInstrumentReport shares a payment instrument.
func (s *PaymentInstrumentReportingServer) SharePaymentInstrumentReport(ctx context.Context, req *paymentsv1.SharePaymentInstrumentReportRequest) (*paymentsv1.SharePaymentInstrumentReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment_instrument", req.GetPaymentInstrumentId(), "share_payment_instrument_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share payment instrument report", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*paymentsv1.SharePaymentInstrumentReportRequest_Details)
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope paymentsv1.PaymentRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	sendSearchIndexTestNoticeRequest := &searchv1.SendSearchIndexTestNoticeRequest{
		Context: req.GetContext(),
		Details: &searchv1.SendSearchIndexTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Phone:            cloned.GetRecipient().GetPhone(),
				PreferredChannel: req.GetDetails().GetRecipient().GetPreferredChannel(),
				Email:            req.GetDetails().GetRecipient().GetEmail(),
				Name:             req.GetDetails().GetRecipient().GetName(),
			},
			Payload: packed,
		},
		SearchIndexId: envelope.GetDisplayName(),
	}
	sendSearchIndexTestNoticeReply, err := s.searchIndexNotificationPort.SendSearchIndexTestNotice(ctx, sendSearchIndexTestNoticeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/SendSearchIndexTestNotice")
	}
	var envelope2 paymentsv1.PaymentRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	listSearchIndexSubscriptionsRequest := &searchv1.ListSearchIndexSubscriptionsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSearchIndexSubscriptionsRequest_Filter{
			Region:       sendSearchIndexTestNoticeReply.GetSearchIndex().GetAddress().GetRegion(),
			CreatedAfter: sendSearchIndexTestNoticeReply.GetSearchIndex().GetAudit().GetUpdatedAt(),
			OwnerId:      sendSearchIndexTestNoticeReply.GetSearchIndex().GetComponents()["primary"].GetComponentId(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope2.GetDisplayName()},
	}
	listSearchIndexSubscriptionsReply, err := s.meteredSearchIndexNotification.ListSearchIndexSubscriptions(ctx, listSearchIndexSubscriptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/ListSearchIndexSubscriptions")
	}
	return &paymentsv1.SharePaymentInstrumentReportResponse{
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Audit: &commonv1.AuditInfo{
				Etag: sendSearchIndexTestNoticeReply.GetSearchIndex().GetAudit().GetEtag(),
			},
			Limits: []*paymentsv1.PaymentInstrumentLimit{&paymentsv1.PaymentInstrumentLimit{
				Name: sendSearchIndexTestNoticeReply.GetSearchIndex().GetName(),
			}},
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			Contact:             &commonv1.ContactInfo{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{
			Details: listSearchIndexSubscriptionsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// CountPaymentInstrumentTotals counts payment instruments by status.
func (s *PaymentInstrumentReportingServer) CountPaymentInstrumentTotals(ctx context.Context, req *paymentsv1.CountPaymentInstrumentTotalsRequest) (*paymentsv1.CountPaymentInstrumentTotalsResponse, error) {
	s.log.InfoContext(ctx, "count payment instrument totals", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "region", req.GetFilter().GetRegion())
	filterCache := cache.New[string, *paymentsv1.CountPaymentInstrumentTotalsRequest_Filter](1)
	filterCache.Put(req.GetContext().GetTraceId(), req.GetFilter())
	cachedFilter, _ := filterCache.Get(req.GetContext().GetTraceId())
	streamSearchIndexNoticesStream, err := s.searchIndexNotificationClient.StreamSearchIndexNotices(ctx, &searchv1.StreamSearchIndexNoticesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.StreamSearchIndexNoticesRequest_Filter{
			Region: tenancy.Value(ctx, "region"),
			Query:  cachedFilter.GetQuery(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/StreamSearchIndexNotices")
	}
	for {
		received, err := streamSearchIndexNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/StreamSearchIndexNotices")
		}
		_ = received
	}
	return &paymentsv1.CountPaymentInstrumentTotalsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// DeletePaymentInstrumentReport deletes a payment instrument.
func (s *PaymentInstrumentReportingServer) DeletePaymentInstrumentReport(ctx context.Context, req *paymentsv1.DeletePaymentInstrumentReportRequest) (*paymentsv1.DeletePaymentInstrumentReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment_instrument", req.GetPaymentInstrumentId(), "delete_payment_instrument_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete payment instrument report", "tenant", req.GetContext().GetTenantId())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "payments.delete_payment_instrument_report.reject_points_ledger",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"reason":                      req.GetReason(),
			"points_ledger.contact.email": req.GetContext().GetActor().GetEmail(),
			"details.approver.user_id":    req.GetContext().GetActor().GetUserId(),
			"context.tenant_id":           req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &paymentsv1.DeletePaymentInstrumentReportResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}
