package shipping

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	chatv1 "github.com/hounddogai/protomap-test-golang/gen/acme/chat/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	deliveriesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/deliveries/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	lendingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/lending/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	timekeepingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/timekeeping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// TrackingEventReportingServer implements `acme.shipping.v1.TrackingEventReportingService`.
// Builds and schedules reports about tracking events.
type TrackingEventReportingServer struct {
	shippingv1.UnsafeTrackingEventReportingServiceServer
	store                        *Store
	log                          *slog.Logger
	conn                         *grpc.ClientConn
	carrierRateValidationClient  shippingv1.CarrierRateValidationServiceClient
	clients                      *registry.Registry
	bus                          *eventbus.Bus
	customerClient               customersv1.CustomerServiceClient
	timesheetQueryClient         timekeepingv1.TimesheetQueryServiceClient
	timesheetQueryClientFallback timekeepingv1.TimesheetQueryServiceClient
	manifestCalculationClient    shippingv1.ManifestCalculationServiceClient
	shipmentValidationOnce       sync.Once
	shipmentValidationCached     shippingv1.ShipmentValidationServiceClient
	customerCalculationClient    customersv1.CustomerCalculationServiceClient
	carrierRateCalculationClient shippingv1.CarrierRateCalculationServiceClient
	chatRoomQueryClient          chatv1.ChatRoomQueryServiceClient
	meteredCarrierRateReporting  meteredCarrierRateReporting
}

// NewTrackingEventReportingServer wires a TrackingEventReportingServer.
func NewTrackingEventReportingServer(deps platform.Deps) *TrackingEventReportingServer {
	return &TrackingEventReportingServer{
		store:                        NewStore(deps.DB),
		log:                          deps.Logger.With("service", "acme.shipping.v1.TrackingEventReportingService"),
		conn:                         deps.Conn,
		carrierRateValidationClient:  shippingv1.NewCarrierRateValidationServiceClient(deps.Conn),
		clients:                      deps.Clients,
		bus:                          deps.Bus,
		customerClient:               customersv1.NewCustomerServiceClient(deps.Conn),
		timesheetQueryClient:         timekeepingv1.NewTimesheetQueryServiceClient(deps.Conn),
		timesheetQueryClientFallback: timekeepingv1.NewTimesheetQueryServiceClient(deps.Conn),
		manifestCalculationClient:    shippingv1.NewManifestCalculationServiceClient(deps.Conn),
		customerCalculationClient:    customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		carrierRateCalculationClient: shippingv1.NewCarrierRateCalculationServiceClient(deps.Conn),
		chatRoomQueryClient:          chatv1.NewChatRoomQueryServiceClient(deps.Conn),
		meteredCarrierRateReporting:  meteredCarrierRateReporting{CarrierRateReportingServiceClient: shippingv1.NewCarrierRateReportingServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}
}

// GenerateTrackingEventReport generates a tracking event.
func (s *TrackingEventReportingServer) GenerateTrackingEventReport(ctx context.Context, req *shippingv1.GenerateTrackingEventReportRequest) (*shippingv1.GenerateTrackingEventReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tracking_event", req.GetTrackingEventId(), "generate_tracking_event_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate tracking event report", "tenant", req.GetContext().GetTenantId())
	explainTrackingEventDecisionRequest := &shippingv1.ExplainTrackingEventDecisionRequest{
		Context:         req.GetContext(),
		Reason:          req.GetReason(),
		TrackingEventId: req.GetTrackingEventId(),
	}
	explainTrackingEventDecisionReply := new(shippingv1.ExplainTrackingEventDecisionResponse)
	if err := s.conn.Invoke(ctx, shippingTrackingEventValidationServiceExplainTrackingEventDecisionMethod, explainTrackingEventDecisionRequest, explainTrackingEventDecisionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventValidationService/ExplainTrackingEventDecision")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	step := &trackingEventReportingServerGenerateTrackingEventReportLintCarrierRatesStep{request: &shippingv1.LintCarrierRatesRequest{
		Context: req.GetContext(),
		CarrierRate: &shippingv1.CarrierRate{
			Owner: &identityv1.UserRef{
				DisplayName: explainTrackingEventDecisionReply.GetTrackingEvent().GetSubject().GetDisplayName(),
			},
		},
		Reason:      req.GetReason(),
		Annotations: annotations,
		Details: &shippingv1.LintCarrierRatesRequest_Details{
			Contact: &commonv1.ContactInfo{Email: cloned.GetActor().GetEmail()},
		},
	}}
	if err := s.runGenerateTrackingEventReportLintCarrierRates(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateValidationService/LintCarrierRates")
	}
	lintCarrierRatesReply := step.reply
	return &shippingv1.GenerateTrackingEventReportResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  explainTrackingEventDecisionReply.GetResult().GetReferenceIds(),
			Status:        explainTrackingEventDecisionReply.GetOutcome().GetStatus(),
			CorrelationId: lintCarrierRatesReply.GetResult().GetCorrelationId(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchTrackingEventReport returns a tracking event.
func (s *TrackingEventReportingServer) FetchTrackingEventReport(ctx context.Context, req *shippingv1.FetchTrackingEventReportRequest) (*shippingv1.FetchTrackingEventReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tracking_event", req.GetTrackingEventId(), "fetch_tracking_event_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch tracking event report", "tenant", req.GetContext().GetTenantId())
	customerCalculation, ok := s.clients.Get("acme.customers.v1.CustomerCalculationService").(customersv1.CustomerCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.customers.v1.CustomerCalculationService")
	}
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	calculateCustomerRequest := &customersv1.CalculateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Contact:             &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			RelationshipManager: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			IdentityDocument:    &customersv1.Customer_Passport{Passport: &customersv1.CustomerPassport{ExpiresAt: (&decoded).GetDeadline()}},
		},
	}
	calculateCustomerReply, err := customerCalculation.CalculateCustomer(ctx, calculateCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/CalculateCustomer")
	}
	explainConsentRecordDecisionRequest := &customersv1.ExplainConsentRecordDecisionRequest{
		Context: req.GetContext(),
		Details: &customersv1.ExplainConsentRecordDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email:            calculateCustomerReply.GetCustomer().GetRelationshipManager().GetEmail(),
				Phone:            calculateCustomerReply.GetCustomer().GetContact().GetPhone(),
				PreferredChannel: calculateCustomerReply.GetCustomer().GetContact().GetPreferredChannel(),
			},
		},
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: calculateCustomerReply.GetCustomer().GetRelationshipManager().GetDisplayName(),
			},
		},
	}
	explainConsentRecordDecisionReply, err := explainConsentRecordDecisionCall(ctx, explainConsentRecordDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ExplainConsentRecordDecision")
	}
	return &shippingv1.FetchTrackingEventReportResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: calculateCustomerReply.GetResult().GetCorrelationId(),
			ReferenceIds:  explainConsentRecordDecisionReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		TrackingEvent: &shippingv1.TrackingEvent{
			CorrelationId:   explainConsentRecordDecisionReply.GetConsentRecord().GetCorrelationId(),
			TrackingEventId: req.GetTrackingEventId(),
		},
	}, nil
}

// ScheduleTrackingEventReport schedules a tracking event.
func (s *TrackingEventReportingServer) ScheduleTrackingEventReport(ctx context.Context, req *shippingv1.ScheduleTrackingEventReportRequest) (*shippingv1.ScheduleTrackingEventReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tracking_event", req.GetTrackingEventId(), "schedule_tracking_event_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule tracking event report", "tenant", req.GetContext().GetTenantId())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "shipping.schedule_tracking_event_report.escalate_loyalty_member",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"details.approver.email": req.GetContext().GetActor().GetEmail(),
			"context.tenant_id":      req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	batchCreateCustomersStream, err := s.customerClient.BatchCreateCustomers(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/BatchCreateCustomers")
	}
	if err := batchCreateCustomersStream.Send(&customersv1.BatchCreateCustomersRequest{Context: req.GetContext()}); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/BatchCreateCustomers")
	}
	if _, err := batchCreateCustomersStream.CloseAndRecv(); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/BatchCreateCustomers")
	}
	return &shippingv1.ScheduleTrackingEventReportResponse{
		Period: req.GetDetails().GetPeriod(),
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// StreamTrackingEventMetrics streams the changes of tracking events as they happen.
func (s *TrackingEventReportingServer) StreamTrackingEventMetrics(req *shippingv1.StreamTrackingEventMetricsRequest, stream grpc.ServerStreamingServer[shippingv1.StreamTrackingEventMetricsResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "stream tracking event metrics", "tenant", req.GetContext().GetTenantId())
	trackingEventCalculation, err := registry.Lookup[shippingv1.TrackingEventCalculationServiceClient](s.clients, "acme.shipping.v1.TrackingEventCalculationService")
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	packed, err := anypb.New(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "pack payload")
	}
	recalculateTrackingEventsRequest := &shippingv1.RecalculateTrackingEventsRequest{
		Context: req.GetContext(),
		Details: &shippingv1.RecalculateTrackingEventsRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		TrackingEvent: &shippingv1.TrackingEvent{
			Measurements: []*shippingv1.TrackingEventMeasurement{&shippingv1.TrackingEventMeasurement{ObservedAt: req.GetSince()}},
			Detail:       packed,
		},
	}
	recalculateTrackingEventsReply, err := trackingEventCalculation.RecalculateTrackingEvents(ctx, recalculateTrackingEventsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.shipping.v1.TrackingEventCalculationService/RecalculateTrackingEvents")
	}
	routes := map[string]func(context.Context, *timekeepingv1.BrowseTimesheetsRequest, ...grpc.CallOption) (*timekeepingv1.BrowseTimesheetsResponse, error){
		"primary":  s.timesheetQueryClient.BrowseTimesheets,
		"fallback": s.timesheetQueryClientFallback.BrowseTimesheets,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	browseTimesheetsRequest := &timekeepingv1.BrowseTimesheetsRequest{
		Context: req.GetContext(),
		Filter: &timekeepingv1.BrowseTimesheetsRequest_Filter{
			Query:  req.GetFilter().GetQuery(),
			Labels: labels,
			Region: req.GetFilter().GetRegion(),
		},
	}
	browseTimesheetsReply, err := route(ctx, browseTimesheetsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.timekeeping.v1.TimesheetQueryService/BrowseTimesheets")
	}
	encoded, err := protojson.Marshal(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "encode request")
	}
	var decoded shippingv1.StreamTrackingEventMetricsRequest_Filter
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return err
	}
	simulateManifestRequest := &shippingv1.SimulateManifestRequest{
		Context: req.GetContext(),
		Manifest: &shippingv1.Manifest{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: recalculateTrackingEventsReply.GetTrackingEvent().GetSubject().GetDisplayName(),
			},
			Number: recalculateTrackingEventsReply.GetTrackingEvent().GetSubject().GetNumber(),
			Total:  recalculateTrackingEventsReply.GetBreakdown().GetTotal(),
		},
		Details: &shippingv1.SimulateManifestRequest_Details{
			Destination: &commonv1.PostalAddress{Region: (&decoded).GetRegion()},
		},
	}
	simulateManifestReply, err := grpcx.Call(ctx, s.manifestCalculationClient.SimulateManifest, simulateManifestRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.shipping.v1.ManifestCalculationService/SimulateManifest")
	}
	scoreShipmentRequest := &shippingv1.ScoreShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			Settlement: &shippingv1.Shipment_Card{Card: &shippingv1.ShipmentCardSettlement{
				Token: simulateManifestReply.GetManifest().GetCard().GetToken(),
			}},
			BillingAddress: &commonv1.PostalAddress{
				Lines: simulateManifestReply.GetManifest().GetBillingAddress().GetLines(),
			},
		},
	}
	scoreShipmentReply, err := s.shipmentValidationLazy().ScoreShipment(ctx, scoreShipmentRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.shipping.v1.ShipmentValidationService/ScoreShipment")
	}
	_ = scoreShipmentReply
	if err := stream.Send(&shippingv1.StreamTrackingEventMetricsResponse{
		Sequence: recalculateTrackingEventsReply.GetTrackingEvent().GetSequence(),
		TrackingEvent: &shippingv1.TrackingEvent{
			Subject: &shippingv1.ShipmentRef{
				ShipmentId: recalculateTrackingEventsReply.GetTrackingEvent().GetSubject().GetShipmentId(),
			},
		},
		Result: &commonv1.OperationResult{
			ReferenceIds:  browseTimesheetsReply.GetResult().GetReferenceIds(),
			Status:        browseTimesheetsReply.GetResult().GetStatus(),
			CorrelationId: simulateManifestReply.GetResult().GetCorrelationId(),
		},
	}); err != nil {
		return err
	}
	return nil
}

// ListTrackingEventReports lists tracking events a page at a time.
func (s *TrackingEventReportingServer) ListTrackingEventReports(ctx context.Context, req *shippingv1.ListTrackingEventReportsRequest) (*shippingv1.ListTrackingEventReportsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListTrackingEventReports", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list tracking event reports", "tenant", req.GetContext().GetTenantId())
	return &shippingv1.ListTrackingEventReportsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ShareTrackingEventReport shares a tracking event.
func (s *TrackingEventReportingServer) ShareTrackingEventReport(ctx context.Context, req *shippingv1.ShareTrackingEventReportRequest) (*shippingv1.ShareTrackingEventReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ShareTrackingEventReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tracking_event", req.GetTrackingEventId(), "share_tracking_event_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share tracking event report", "tenant", req.GetContext().GetTenantId())
	explainCustomerCalculationRequest := buildExplainCustomerCalculationRequestForShareTrackingEventReport(req)
	explainCustomerCalculationReply, err := s.customerCalculationClient.ExplainCustomerCalculation(ctx, explainCustomerCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/ExplainCustomerCalculation")
	}
	createLoanApplicationRequest := &lendingv1.CreateLoanApplicationRequest{
		Context: req.GetContext(),
		LoanApplication: &lendingv1.LoanApplication{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: explainCustomerCalculationReply.GetCustomer().GetAudit().GetUpdatedAt(),
			},
			Counterparty: &customersv1.CustomerRef{
				Email: explainCustomerCalculationReply.GetCustomer().GetRelationshipManager().GetEmail(),
			},
		},
	}
	createLoanApplicationReply, err := createLoanApplicationCall(ctx, createLoanApplicationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.lending.v1.LoanApplicationService/CreateLoanApplication")
	}
	quoteCarrierRateRequest := &shippingv1.QuoteCarrierRateRequest{
		Context: req.GetContext(),
		Details: &shippingv1.QuoteCarrierRateRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: createLoanApplicationReply.GetLoanApplication().GetBillingAddress().GetRecipient(),
			},
		},
		CarrierRate: &shippingv1.CarrierRate{
			Audit: &commonv1.AuditInfo{
				Etag: createLoanApplicationReply.GetLoanApplication().GetAudit().GetEtag(),
			},
		},
	}
	quoteCarrierRateReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*shippingv1.QuoteCarrierRateResponse, error) {
		return s.carrierRateCalculationClient.QuoteCarrierRate(ctx, quoteCarrierRateRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateCalculationService/QuoteCarrierRate")
	}
	householdValidation, err := registry.Lookup[customersv1.HouseholdValidationServiceClient](s.clients, "acme.customers.v1.HouseholdValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	normalizeHouseholdRequest := &customersv1.NormalizeHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit: &commonv1.AuditInfo{
						Etag:      createLoanApplicationReply.GetLoanApplication().GetAudit().GetEtag(),
						UpdatedAt: createLoanApplicationReply.GetLoanApplication().GetAudit().GetUpdatedAt(),
					},
					Location: &commonv1.PostalAddress{
						Recipient: explainCustomerCalculationReply.GetCustomer().GetPrimaryAddress().GetRecipient(),
					},
				}},
			}},
		},
	}
	normalizeHouseholdReply, err := householdValidation.NormalizeHousehold(ctx, normalizeHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/NormalizeHousehold")
	}
	return &shippingv1.ShareTrackingEventReportResponse{
		TrackingEvent: &shippingv1.TrackingEvent{
			Subject: &shippingv1.ShipmentRef{
				DisplayName: explainCustomerCalculationReply.GetCustomer().GetRelationshipManager().GetDisplayName(),
			},
			TrackingEventId: req.GetTrackingEventId(),
		},
		Result: &commonv1.OperationResult{
			Details: createLoanApplicationReply.GetResult().GetDetails(),
			Status:  normalizeHouseholdReply.GetOutcome().GetStatus(),
		},
		Outcome: &shippingv1.ShareTrackingEventReportResponse_Outcome{
			Status:    quoteCarrierRateReply.GetResult().GetStatus(),
			NextSteps: normalizeHouseholdReply.GetOutcome().GetNextSteps(),
		},
	}, nil
}

// CountTrackingEventTotals counts tracking events by status.
func (s *TrackingEventReportingServer) CountTrackingEventTotals(ctx context.Context, req *shippingv1.CountTrackingEventTotalsRequest) (*shippingv1.CountTrackingEventTotalsResponse, error) {
	s.log.InfoContext(ctx, "count tracking event totals", "tenant", req.GetContext().GetTenantId())
	exportChatRoomsStream, err := s.chatRoomQueryClient.ExportChatRooms(ctx, &chatv1.ExportChatRoomsRequest{
		Context: req.GetContext(),
		Filter: &chatv1.ExportChatRoomsRequest_Filter{
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			OwnerId:      req.GetFilter().GetOwnerId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.chat.v1.ChatRoomQueryService/ExportChatRooms")
	}
	for {
		received, err := exportChatRoomsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.chat.v1.ChatRoomQueryService/ExportChatRooms")
		}
		_ = received
	}
	return &shippingv1.CountTrackingEventTotalsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// DeleteTrackingEventReport deletes a tracking event.
func (s *TrackingEventReportingServer) DeleteTrackingEventReport(ctx context.Context, req *shippingv1.DeleteTrackingEventReportRequest) (*shippingv1.DeleteTrackingEventReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected DeleteTrackingEventReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tracking_event", req.GetTrackingEventId(), "delete_tracking_event_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete tracking event report", "tenant", req.GetContext().GetTenantId())
	countDeliveriesRequest := &deliveriesv1.CountDeliveriesRequest{
		Context: req.GetContext(),
		Filter: &deliveriesv1.CountDeliveriesRequest_Filter{
			OwnerId:      req.GetTrackingEventId(),
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	}
	countDeliveriesReply := new(deliveriesv1.CountDeliveriesResponse)
	if err := s.conn.Invoke(ctx, deliveriesDeliveryQueryServiceCountDeliveriesMethod, countDeliveriesRequest, countDeliveriesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.deliveries.v1.DeliveryQueryService/CountDeliveries")
	}
	countCarrierRateTotalsRequest := &shippingv1.CountCarrierRateTotalsRequest{
		Context: req.GetContext(),
		Filter: &shippingv1.CountCarrierRateTotalsRequest_Filter{
			Labels:  countDeliveriesReply.GetResult().GetDetails(),
			OwnerId: req.GetContext().GetActor().GetUserId(),
		},
	}
	countCarrierRateTotalsReply, err := s.meteredCarrierRateReporting.CountCarrierRateTotals(ctx, countCarrierRateTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateReportingService/CountCarrierRateTotals")
	}
	return &shippingv1.DeleteTrackingEventReportResponse{
		Result: &commonv1.OperationResult{
			Message:       countDeliveriesReply.GetResult().GetMessage(),
			ReferenceIds:  countDeliveriesReply.GetResult().GetReferenceIds(),
			CorrelationId: countCarrierRateTotalsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// trackingEventReportingServerGenerateTrackingEventReportLintCarrierRatesStep carries the request of one step of
// GenerateTrackingEventReport and, once it ran, its reply.
type trackingEventReportingServerGenerateTrackingEventReportLintCarrierRatesStep struct {
	request *shippingv1.LintCarrierRatesRequest
	reply   *shippingv1.LintCarrierRatesResponse
}

func (s *TrackingEventReportingServer) runGenerateTrackingEventReportLintCarrierRates(ctx context.Context, step *trackingEventReportingServerGenerateTrackingEventReportLintCarrierRatesStep) error {
	reply, err := s.carrierRateValidationClient.LintCarrierRates(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// shipmentValidationLazy dials acme.shipping.v1.ShipmentValidationService the first time a handler needs it.
func (s *TrackingEventReportingServer) shipmentValidationLazy() shippingv1.ShipmentValidationServiceClient {
	s.shipmentValidationOnce.Do(func() { s.shipmentValidationCached = shippingv1.NewShipmentValidationServiceClient(s.conn) })
	return s.shipmentValidationCached
}
