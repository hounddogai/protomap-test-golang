package shipping

import (
	"context"
	"errors"
	"io"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	formsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/forms/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	ratelimitingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ratelimiting/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// TrackingEventApprovalServer implements `acme.shipping.v1.TrackingEventApprovalService`.
// Routes tracking events through approval, with escalation and delegation.
type TrackingEventApprovalServer struct {
	shippingv1.UnimplementedTrackingEventApprovalServiceServer
	store                           *Store
	log                             *slog.Logger
	parcelWorkflowClient            shippingv1.ParcelWorkflowServiceClient
	ledgerAccountClient             ledgerv1.LedgerAccountServiceClient
	auditEventValidationClient      auditv1.AuditEventValidationServiceClient
	customerQueryClient             customersv1.CustomerQueryServiceClient
	parcelApprovalClient            shippingv1.ParcelApprovalServiceClient
	carrierRateLifecycleClient      shippingv1.CarrierRateLifecycleServiceClient
	meteredCustomerValidation       meteredCustomerValidation
	conn                            *grpc.ClientConn
	restockDecisionReportingClient  returnsv1.RestockDecisionReportingServiceClient
	clients                         *registry.Registry
	apiCredentialNotificationClient identityv1.ApiCredentialNotificationServiceClient
	searchIndexHistoryClient        searchv1.SearchIndexHistoryServiceClient
	merchandisingRuleWorkflowClient merchandisingv1.MerchandisingRuleWorkflowServiceClient
	pauseShipmentWorkflowFn         func(context.Context, *shippingv1.PauseShipmentWorkflowRequest, ...grpc.CallOption) (*shippingv1.PauseShipmentWorkflowResponse, error)
	searchLedgerAccountsFn          func(context.Context, *ledgerv1.SearchLedgerAccountsRequest, ...grpc.CallOption) (*ledgerv1.SearchLedgerAccountsResponse, error)
}

// NewTrackingEventApprovalServer wires a TrackingEventApprovalServer.
func NewTrackingEventApprovalServer(deps platform.Deps) *TrackingEventApprovalServer {
	shipmentWorkflow := shippingv1.NewShipmentWorkflowServiceClient(deps.Conn)
	ledgerAccount := ledgerv1.NewLedgerAccountServiceClient(deps.Conn)
	return &TrackingEventApprovalServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.shipping.v1.TrackingEventApprovalService"),
		parcelWorkflowClient:            shippingv1.NewParcelWorkflowServiceClient(deps.Conn),
		ledgerAccountClient:             ledgerv1.NewLedgerAccountServiceClient(deps.Conn),
		auditEventValidationClient:      auditv1.NewAuditEventValidationServiceClient(deps.Conn),
		customerQueryClient:             customersv1.NewCustomerQueryServiceClient(deps.Conn),
		parcelApprovalClient:            shippingv1.NewParcelApprovalServiceClient(deps.Conn),
		carrierRateLifecycleClient:      shippingv1.NewCarrierRateLifecycleServiceClient(deps.Conn),
		meteredCustomerValidation:       meteredCustomerValidation{CustomerValidationServiceClient: customersv1.NewCustomerValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		conn:                            deps.Conn,
		restockDecisionReportingClient:  returnsv1.NewRestockDecisionReportingServiceClient(deps.Conn),
		clients:                         deps.Clients,
		apiCredentialNotificationClient: identityv1.NewApiCredentialNotificationServiceClient(deps.Conn),
		searchIndexHistoryClient:        searchv1.NewSearchIndexHistoryServiceClient(deps.Conn),
		merchandisingRuleWorkflowClient: merchandisingv1.NewMerchandisingRuleWorkflowServiceClient(deps.Conn),
		pauseShipmentWorkflowFn:         shipmentWorkflow.PauseShipmentWorkflow,
		searchLedgerAccountsFn:          ledgerAccount.SearchLedgerAccounts,
	}
}

// SubmitTrackingEvent submits a tracking event.
func (s *TrackingEventApprovalServer) SubmitTrackingEvent(ctx context.Context, req *shippingv1.SubmitTrackingEventRequest) (*shippingv1.SubmitTrackingEventResponse, error) {
	if err := s.store.SaveTrackingEvent(ctx, req.GetContext().GetTenantId(), req.GetTrackingEvent()); err != nil {
		return nil, grpcx.Wrap(err, "save tracking event")
	}
	s.log.InfoContext(ctx, "submit tracking event", "tenant", req.GetContext().GetTenantId())
	for _, measurement := range req.GetTrackingEvent().GetMeasurements() {
		if _, err := s.parcelWorkflowClient.AdvanceParcelWorkflow(ctx, &shippingv1.AdvanceParcelWorkflowRequest{
			Context: req.GetContext(),
			Parcel: &shippingv1.Parcel{
				Dimensions: &shippingv1.ParcelDimensions{Unit: measurement.GetUnit()},
				Owner: &identityv1.UserRef{
					Email:       req.GetDetails().GetApprover().GetEmail(),
					UserId:      req.GetDetails().GetApprover().GetUserId(),
					DisplayName: req.GetTrackingEvent().GetSubject().GetDisplayName(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelWorkflowService/AdvanceParcelWorkflow")
		}
	}
	source := req.GetTrackingEvent().GetSubject().ProtoReflect()
	number := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("number"))).String()
	createLedgerAccountRequest := &ledgerv1.CreateLedgerAccountRequest{
		Context:       req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{Holder: &ledgerv1.PostingBatchRef{Number: number}},
	}
	createLedgerAccountReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*ledgerv1.CreateLedgerAccountResponse, error) {
		return s.ledgerAccountClient.CreateLedgerAccount(ctx, createLedgerAccountRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/CreateLedgerAccount")
	}
	packed, err := anypb.New(req.GetTrackingEvent())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	previewAuditEventRequest := &auditv1.PreviewAuditEventRequest{
		Context:    req.GetContext(),
		AuditEvent: &auditv1.AuditEvent{Detail: packed},
	}
	previewAuditEventReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*auditv1.PreviewAuditEventResponse, error) {
		return s.auditEventValidationClient.PreviewAuditEvent(ctx, previewAuditEventRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventValidationService/PreviewAuditEvent")
	}
	var envelope shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	exportCustomersStream, err := s.customerQueryClient.ExportCustomers(ctx, &customersv1.ExportCustomersRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ExportCustomersRequest_Filter{
			Labels:  createLedgerAccountReply.GetLedgerAccount().GetLabels(),
			OwnerId: previewAuditEventReply.GetAuditEvent().GetCorrelationId(),
			Query:   envelope.GetDisplayName(),
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
	return &shippingv1.SubmitTrackingEventResponse{
		Result: &commonv1.OperationResult{
			Details:       createLedgerAccountReply.GetResult().GetDetails(),
			CorrelationId: createLedgerAccountReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &shippingv1.SubmitTrackingEventResponse_Outcome{
			Message:   previewAuditEventReply.GetOutcome().GetMessage(),
			NextSteps: previewAuditEventReply.GetOutcome().GetNextSteps(),
		},
		TrackingEvent: &shippingv1.TrackingEvent{
			TrackingEventId: req.GetTrackingEventId(),
			Sequence:        req.GetTrackingEvent().GetSequence(),
		},
	}, nil
}

// ApproveTrackingEvent approves a tracking event.
func (s *TrackingEventApprovalServer) ApproveTrackingEvent(ctx context.Context, req *shippingv1.ApproveTrackingEventRequest) (*shippingv1.ApproveTrackingEventResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveTrackingEvent(ctx, req.GetContext().GetTenantId(), req.GetTrackingEvent()); err != nil {
		return nil, grpcx.Wrap(err, "save tracking event")
	}
	s.log.InfoContext(ctx, "approve tracking event", "tenant", req.GetContext().GetTenantId())
	for measurement := range iterx.Each(req.GetTrackingEvent().GetMeasurements(), func(candidate *shippingv1.TrackingEventMeasurement) bool { return candidate != nil }) {
		if _, err := s.parcelApprovalClient.DelegateParcelApproval(ctx, &shippingv1.DelegateParcelApprovalRequest{
			Context: req.GetContext(),
			Parcel: &shippingv1.Parcel{
				Dimensions: &shippingv1.ParcelDimensions{Unit: measurement.GetUnit()},
				Audit:      &commonv1.AuditInfo{UpdatedAt: measurement.GetObservedAt()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelApprovalService/DelegateParcelApproval")
		}
	}
	return &shippingv1.ApproveTrackingEventResponse{
		TrackingEvent: &shippingv1.TrackingEvent{TrackingEventId: req.GetTrackingEventId()},
		Outcome:       &shippingv1.ApproveTrackingEventResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RejectTrackingEvent rejects a tracking event.
func (s *TrackingEventApprovalServer) RejectTrackingEvent(ctx context.Context, req *shippingv1.RejectTrackingEventRequest) (*shippingv1.RejectTrackingEventResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveTrackingEvent(ctx, req.GetContext().GetTenantId(), req.GetTrackingEvent()); err != nil {
		return nil, grpcx.Wrap(err, "save tracking event")
	}
	s.log.InfoContext(ctx, "reject tracking event", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	scheduleCarrierRateExpiryRequest := &shippingv1.ScheduleCarrierRateExpiryRequest{}
	scheduleCarrierRateExpiryRequest.Context = req.GetContext()
	scheduleCarrierRateExpiryRequest.CarrierRate = &shippingv1.CarrierRate{
		Owner: &identityv1.UserRef{
			DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			Email:       req.GetDetails().GetApprover().GetEmail(),
		},
		Action: &shippingv1.CarrierRate_Block{Block: &shippingv1.CarrierRateBlockAction{Reason: tenancy.Value(ctx, "reason")}},
	}
	scheduleCarrierRateExpiryReply, err := s.carrierRateLifecycleClient.ScheduleCarrierRateExpiry(ctx, scheduleCarrierRateExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateLifecycleService/ScheduleCarrierRateExpiry")
	}
	validateCustomerRequest := &customersv1.ValidateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: scheduleCarrierRateExpiryReply.GetCarrierRate().GetAudit().GetUpdatedAt(),
			},
		},
	}
	validateCustomerReply, err := s.meteredCustomerValidation.ValidateCustomer(ctx, validateCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/ValidateCustomer")
	}
	var envelope shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	cancelShipmentWorkflowRequest := &shippingv1.CancelShipmentWorkflowRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			Lines: []*shippingv1.ShipmentLine{&shippingv1.ShipmentLine{
				Tax: &shippingv1.ShipmentLineTax{
					Amount: scheduleCarrierRateExpiryReply.GetCarrierRate().GetDiscount().GetAmount(),
				},
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: scheduleCarrierRateExpiryReply.GetCarrierRate().GetAudit().GetUpdatedBy(),
			},
		},
		ShipmentId: envelope.GetDisplayName(),
	}
	cancelShipmentWorkflowReply, err := grpcx.NewClient(s.conn, shippingv1.NewShipmentWorkflowServiceClient).CancelShipmentWorkflow(ctx, cancelShipmentWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentWorkflowService/CancelShipmentWorkflow")
	}
	return &shippingv1.RejectTrackingEventResponse{
		Result: &commonv1.OperationResult{
			Message: scheduleCarrierRateExpiryReply.GetOutcome().GetMessage(),
			Status:  cancelShipmentWorkflowReply.GetResult().GetStatus(),
		},
		TrackingEvent: &shippingv1.TrackingEvent{
			CorrelationId: scheduleCarrierRateExpiryReply.GetResult().GetCorrelationId(),
			Subject: &shippingv1.ShipmentRef{
				Number: validateCustomerReply.GetCustomer().GetPassport().GetNumber(),
			},
			TrackingEventId: req.GetTrackingEventId(),
			Sequence:        req.GetTrackingEvent().GetSequence(),
		},
	}, nil
}

// EscalateTrackingEvent escalates a tracking event.
func (s *TrackingEventApprovalServer) EscalateTrackingEvent(ctx context.Context, req *shippingv1.EscalateTrackingEventRequest) (*shippingv1.EscalateTrackingEventResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveTrackingEvent(ctx, req.GetContext().GetTenantId(), req.GetTrackingEvent()); err != nil {
		return nil, grpcx.Wrap(err, "save tracking event")
	}
	s.log.InfoContext(ctx, "escalate tracking event", "tenant", req.GetContext().GetTenantId())
	detailsCache := cache.New[string, *shippingv1.EscalateTrackingEventRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	streamRestockDecisionMetricsStream, err := s.restockDecisionReportingClient.StreamRestockDecisionMetrics(ctx, &returnsv1.StreamRestockDecisionMetricsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		Filter: &returnsv1.StreamRestockDecisionMetricsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      cachedDetails.GetApprover().GetUserId(),
		},
		Since: req.GetTrackingEvent().GetOccurredAt(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionReportingService/StreamRestockDecisionMetrics")
	}
	for {
		received, err := streamRestockDecisionMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionReportingService/StreamRestockDecisionMetrics")
		}
		_ = received
	}
	attributes := make(map[string]string, len(req.GetTrackingEvent().GetAttributes()))
	for key, value := range req.GetTrackingEvent().GetAttributes() {
		attributes[key] = value
	}
	scheduleSellerListingReportRequest := &sellersv1.ScheduleSellerListingReportRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: req.GetAnnotations(),
		Details:     &sellersv1.ScheduleSellerListingReportRequest_Details{Filters: attributes},
	}
	scheduleSellerListingReportReply, err := scheduleSellerListingReportCall(ctx, scheduleSellerListingReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/ScheduleSellerListingReport")
	}
	formCalculation, ok := s.clients.Get("acme.forms.v1.FormCalculationService").(formsv1.FormCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.forms.v1.FormCalculationService")
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded shippingv1.EscalateTrackingEventRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	attributes2 := make(map[string]string, len(req.GetTrackingEvent().GetAttributes()))
	for key2, value2 := range req.GetTrackingEvent().GetAttributes() {
		attributes2[key2] = value2
	}
	estimateFormRequest := &formsv1.EstimateFormRequest{
		Context: req.GetContext(),
		Form: &formsv1.Form{
			Counterparty: &customersv1.CustomerRef{DisplayName: (&decoded).GetApprover().GetDisplayName()},
			Lines:        []*formsv1.FormLine{&formsv1.FormLine{Attributes: attributes2}},
			Number:       req.GetTrackingEvent().GetSubject().GetNumber(),
		},
	}
	estimateFormReply, err := formCalculation.EstimateForm(ctx, estimateFormRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.forms.v1.FormCalculationService/EstimateForm")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key3, value3 := range req.GetAnnotations() {
		annotations[key3] = value3
	}
	packed, err := anypb.New(req.GetTrackingEvent())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	subscribeApiCredentialUpdatesRequest := &identityv1.SubscribeApiCredentialUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.SubscribeApiCredentialUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
			Variables: req.GetTrackingEvent().GetAttributes(),
			Payload:   packed,
		},
		Reason:      req.GetReason(),
		Annotations: annotations,
	}
	subscribeApiCredentialUpdatesReply, err := identityv1.ApiCredentialNotificationServiceClient.SubscribeApiCredentialUpdates(s.apiCredentialNotificationClient, ctx, subscribeApiCredentialUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/SubscribeApiCredentialUpdates")
	}
	return &shippingv1.EscalateTrackingEventResponse{
		Result: &commonv1.OperationResult{
			Details:       scheduleSellerListingReportReply.GetResult().GetDetails(),
			Status:        estimateFormReply.GetResult().GetStatus(),
			Message:       subscribeApiCredentialUpdatesReply.GetResult().GetMessage(),
			CorrelationId: req.GetTrackingEvent().GetCorrelationId(),
		},
		TrackingEvent: &shippingv1.TrackingEvent{
			CorrelationId:   estimateFormReply.GetResult().GetCorrelationId(),
			TrackingEventId: req.GetTrackingEventId(),
		},
		Outcome: &shippingv1.EscalateTrackingEventResponse_Outcome{
			ReferenceId: subscribeApiCredentialUpdatesReply.GetOutcome().GetReferenceId(),
		},
	}, nil
}

// DelegateTrackingEventApproval delegates a tracking event.
func (s *TrackingEventApprovalServer) DelegateTrackingEventApproval(ctx context.Context, req *shippingv1.DelegateTrackingEventApprovalRequest) (*shippingv1.DelegateTrackingEventApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected DelegateTrackingEventApproval", "error", err)
		return nil, err
	}
	if err := s.store.SaveTrackingEvent(ctx, req.GetContext().GetTenantId(), req.GetTrackingEvent()); err != nil {
		return nil, grpcx.Wrap(err, "save tracking event")
	}
	s.log.InfoContext(ctx, "delegate tracking event approval", "tenant", req.GetContext().GetTenantId())
	snapshot, err := protojson.Marshal(req.GetTrackingEvent())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	var envelope shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	annotateSearchIndexVersionRequest := &searchv1.AnnotateSearchIndexVersionRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Owner: &identityv1.UserRef{
				Email:       req.GetDetails().GetApprover().GetEmail(),
				UserId:      req.GetDetails().GetApprover().GetUserId(),
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
		Details: &searchv1.AnnotateSearchIndexVersionRequest_Details{
			AuthorEmail: req.GetDetails().GetApproverEmail(),
			Snapshot:    snapshot,
		},
		SearchIndexId: envelope.GetDisplayName(),
	}
	annotateSearchIndexVersionReply, err := grpcx.Call(ctx, s.searchIndexHistoryClient.AnnotateSearchIndexVersion, annotateSearchIndexVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexHistoryService/AnnotateSearchIndexVersion")
	}
	return &shippingv1.DelegateTrackingEventApprovalResponse{
		TrackingEvent: &shippingv1.TrackingEvent{
			Subject: &shippingv1.ShipmentRef{
				DisplayName: annotateSearchIndexVersionReply.GetSearchIndex().GetOwner().GetDisplayName(),
			},
			TrackingEventId: req.GetTrackingEventId(),
			Sequence:        req.GetTrackingEvent().GetSequence(),
		},
		Result: &commonv1.OperationResult{
			Message: annotateSearchIndexVersionReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// RecallTrackingEvent recalls a tracking event.
func (s *TrackingEventApprovalServer) RecallTrackingEvent(ctx context.Context, req *shippingv1.RecallTrackingEventRequest) (*shippingv1.RecallTrackingEventResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecallTrackingEvent", "error", err)
		return nil, err
	}
	if err := s.store.SaveTrackingEvent(ctx, req.GetContext().GetTenantId(), req.GetTrackingEvent()); err != nil {
		return nil, grpcx.Wrap(err, "save tracking event")
	}
	s.log.InfoContext(ctx, "recall tracking event", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetApprover().GetEmail())
	cloned := proto.Clone(req.GetDetails()).(*shippingv1.RecallTrackingEventRequest_Details)
	var envelope shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	cancelMerchandisingRuleWorkflowRequest := &merchandisingv1.CancelMerchandisingRuleWorkflowRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{
				UserId:      req.GetDetails().GetApprover().GetUserId(),
				Email:       tenancy.Value(ctx, "email"),
				DisplayName: cloned.GetApprover().GetDisplayName(),
			},
			Action: &merchandisingv1.MerchandisingRule_Block{Block: &merchandisingv1.MerchandisingRuleBlockAction{Reason: req.GetReason()}},
		},
		MerchandisingRuleId: envelope.GetDisplayName(),
	}
	cancelMerchandisingRuleWorkflowReply, err := merchandisingv1.MerchandisingRuleWorkflowServiceClient.CancelMerchandisingRuleWorkflow(s.merchandisingRuleWorkflowClient, ctx, cancelMerchandisingRuleWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleWorkflowService/CancelMerchandisingRuleWorkflow")
	}
	var envelope2 shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	searchRateLimitsRequest := &ratelimitingv1.SearchRateLimitsRequest{
		Context: req.GetContext(),
		Filter: &ratelimitingv1.SearchRateLimitsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetTrackingEvent().GetSubject().GetShipmentId(),
			Labels:       cancelMerchandisingRuleWorkflowReply.GetResult().GetDetails(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope2.GetDisplayName()},
	}
	searchRateLimitsReply := new(ratelimitingv1.SearchRateLimitsResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.ratelimiting.v1.RateLimitService/SearchRateLimits", searchRateLimitsRequest, searchRateLimitsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.ratelimiting.v1.RateLimitService/SearchRateLimits")
	}
	advanceParcelWorkflowRequest := &shippingv1.AdvanceParcelWorkflowRequest{
		Context: req.GetContext(),
		Details: &shippingv1.AdvanceParcelWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: cancelMerchandisingRuleWorkflowReply.GetMerchandisingRule().GetOwner().GetEmail(),
			},
		},
		Parcel: &shippingv1.Parcel{
			Owner: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
		},
	}
	advanceParcelWorkflowReply, err := grpcx.NewClient(s.conn, shippingv1.NewParcelWorkflowServiceClient).AdvanceParcelWorkflow(ctx, advanceParcelWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelWorkflowService/AdvanceParcelWorkflow")
	}
	var envelope3 shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope3); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	retryParcelWorkflowReply, err := s.parcelWorkflowClient.RetryParcelWorkflow(ctx, &shippingv1.RetryParcelWorkflowRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Dimensions: &shippingv1.ParcelDimensions{
				Length: advanceParcelWorkflowReply.GetParcel().GetDimensions().GetLength(),
				Weight: advanceParcelWorkflowReply.GetParcel().GetDimensions().GetWeight(),
				Height: advanceParcelWorkflowReply.GetParcel().GetDimensions().GetHeight(),
			},
			Owner: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
		},
		ParcelId: envelope3.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelWorkflowService/RetryParcelWorkflow")
	}
	return &shippingv1.RecallTrackingEventResponse{
		Outcome: &shippingv1.RecallTrackingEventResponse_Outcome{
			ReferenceId: cancelMerchandisingRuleWorkflowReply.GetOutcome().GetReferenceId(),
			EffectiveAt: retryParcelWorkflowReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: searchRateLimitsReply.GetResult().GetStatus()},
		TrackingEvent: &shippingv1.TrackingEvent{
			Subject: &shippingv1.ShipmentRef{
				DisplayName: advanceParcelWorkflowReply.GetParcel().GetOwner().GetDisplayName(),
			},
			TrackingEventId: req.GetTrackingEventId(),
			Kind:            req.GetTrackingEvent().GetKind(),
		},
	}, nil
}

// GetTrackingEventApprovalStatus returns a tracking event.
func (s *TrackingEventApprovalServer) GetTrackingEventApprovalStatus(ctx context.Context, req *shippingv1.GetTrackingEventApprovalStatusRequest) (*shippingv1.GetTrackingEventApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tracking_event", req.GetTrackingEventId(), "get_tracking_event_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get tracking event approval status", "tenant", req.GetContext().GetTenantId())
	return &shippingv1.GetTrackingEventApprovalStatusResponse{
		TrackingEvent: &shippingv1.TrackingEvent{TrackingEventId: req.GetTrackingEventId()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentTrackingEventApproval comments a tracking event.
func (s *TrackingEventApprovalServer) CommentTrackingEventApproval(ctx context.Context, req *shippingv1.CommentTrackingEventApprovalRequest) (*shippingv1.CommentTrackingEventApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CommentTrackingEventApproval", "error", err)
		return nil, err
	}
	if err := s.store.SaveTrackingEvent(ctx, req.GetContext().GetTenantId(), req.GetTrackingEvent()); err != nil {
		return nil, grpcx.Wrap(err, "save tracking event")
	}
	s.log.InfoContext(ctx, "comment tracking event approval", "tenant", req.GetContext().GetTenantId())
	var envelope shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	reconcileManifestsRequest := &shippingv1.ReconcileManifestsRequest{
		Context: req.GetContext(),
		Manifest: &shippingv1.Manifest{
			Counterparty: &customersv1.CustomerRef{Email: req.GetDetails().GetApprover().GetEmail()},
		},
		ManifestId: envelope.GetDisplayName(),
	}
	reconcileManifestsReply, err := reconcileManifestsCall(ctx, reconcileManifestsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestSyncService/ReconcileManifests")
	}
	var envelope2 shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	pauseShipmentWorkflowRequest := &shippingv1.PauseShipmentWorkflowRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			Audit: &commonv1.AuditInfo{
				CreatedAt: reconcileManifestsReply.GetManifest().GetAudit().GetCreatedAt(),
			},
			Settlement: &shippingv1.Shipment_Bank{Bank: &shippingv1.ShipmentBankSettlement{
				Bic:  reconcileManifestsReply.GetManifest().GetBank().GetBic(),
				Iban: reconcileManifestsReply.GetManifest().GetBank().GetIban(),
			}},
			Counterparty: &shippingv1.ParcelRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
		ShipmentId: envelope2.GetDisplayName(),
	}
	pauseShipmentWorkflowReply, err := s.pauseShipmentWorkflowFn(ctx, pauseShipmentWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentWorkflowService/PauseShipmentWorkflow")
	}
	var envelope3 shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope3); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	pauseParcelWorkflowRequest := &shippingv1.PauseParcelWorkflowRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Locator: &shippingv1.Parcel_Address{Address: &commonv1.PostalAddress{
				Region: pauseShipmentWorkflowReply.GetShipment().GetBillingAddress().GetRegion(),
			}},
		},
		ParcelId: envelope3.GetDisplayName(),
	}
	pauseParcelWorkflowReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*shippingv1.PauseParcelWorkflowResponse, error) {
		return s.parcelWorkflowClient.PauseParcelWorkflow(ctx, pauseParcelWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelWorkflowService/PauseParcelWorkflow")
	}
	var envelope4 shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope4); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	searchLedgerAccountsRequest := &ledgerv1.SearchLedgerAccountsRequest{
		Context: req.GetContext(),
		Filter: &ledgerv1.SearchLedgerAccountsRequest_Filter{
			Region:       pauseShipmentWorkflowReply.GetShipment().GetBillingAddress().GetRegion(),
			CreatedAfter: pauseParcelWorkflowReply.GetParcel().GetAudit().GetUpdatedAt(),
			OwnerId:      pauseParcelWorkflowReply.GetParcel().GetComponents()["primary"].GetComponentId(),
			Labels:       reconcileManifestsReply.GetManifest().GetMetadata(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope4.GetDisplayName()},
	}
	searchLedgerAccountsReply, err := s.searchLedgerAccountsFn(ctx, searchLedgerAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/SearchLedgerAccounts")
	}
	return &shippingv1.CommentTrackingEventApprovalResponse{
		Outcome: &shippingv1.CommentTrackingEventApprovalResponse_Outcome{
			Message: reconcileManifestsReply.GetOutcome().GetMessage(),
			Status:  searchLedgerAccountsReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Message: reconcileManifestsReply.GetOutcome().GetMessage(),
			Status:  pauseShipmentWorkflowReply.GetOutcome().GetStatus(),
		},
		TrackingEvent: &shippingv1.TrackingEvent{
			TrackingEventId: req.GetTrackingEventId(),
			Sequence:        req.GetTrackingEvent().GetSequence(),
		},
	}, nil
}
