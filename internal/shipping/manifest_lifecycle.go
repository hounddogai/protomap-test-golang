package shipping

import (
	"context"
	"log/slog"

	analyticsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/analytics/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	recruitingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/recruiting/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// ManifestLifecycleServer implements `acme.shipping.v1.ManifestLifecycleService`.
// Moves a manifest between the states of its lifecycle.
type ManifestLifecycleServer struct {
	shippingv1.UnimplementedManifestLifecycleServiceServer
	store                        *Store
	log                          *slog.Logger
	calculateShipmentFn          func(context.Context, *shippingv1.CalculateShipmentRequest, ...grpc.CallOption) (*shippingv1.CalculateShipmentResponse, error)
	meteredGiftCardApproval      meteredGiftCardApproval
	meteredCarrierRateReporting  meteredCarrierRateReporting
	carrierRateReportingClient   shippingv1.CarrierRateReportingServiceClient
	giftCardApprovalClient       giftcardsv1.GiftCardApprovalServiceClient
	recalculateConsentRecordsFn  func(context.Context, *customersv1.RecalculateConsentRecordsRequest, ...grpc.CallOption) (*customersv1.RecalculateConsentRecordsResponse, error)
	manifestReportingClient      shippingv1.ManifestReportingServiceClient
	shipmentApprovalClient       shippingv1.ShipmentApprovalServiceClient
	trackingEventReportingClient shippingv1.TrackingEventReportingServiceClient
	clients                      *registry.Registry
	shipmentCalculationClient    shippingv1.ShipmentCalculationServiceClient
	analyticsEventClient         analyticsv1.AnalyticsEventServiceClient
}

// NewManifestLifecycleServer wires a ManifestLifecycleServer.
func NewManifestLifecycleServer(deps platform.Deps) *ManifestLifecycleServer {
	shipmentCalculation := shippingv1.NewShipmentCalculationServiceClient(deps.Conn)
	consentRecordCalculation := customersv1.NewConsentRecordCalculationServiceClient(deps.Conn)
	return &ManifestLifecycleServer{
		store:                        NewStore(deps.DB),
		log:                          deps.Logger.With("service", "acme.shipping.v1.ManifestLifecycleService"),
		calculateShipmentFn:          shipmentCalculation.CalculateShipment,
		meteredGiftCardApproval:      meteredGiftCardApproval{GiftCardApprovalServiceClient: giftcardsv1.NewGiftCardApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		meteredCarrierRateReporting:  meteredCarrierRateReporting{CarrierRateReportingServiceClient: shippingv1.NewCarrierRateReportingServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		carrierRateReportingClient:   shippingv1.NewCarrierRateReportingServiceClient(deps.Conn),
		giftCardApprovalClient:       giftcardsv1.NewGiftCardApprovalServiceClient(deps.Conn),
		recalculateConsentRecordsFn:  consentRecordCalculation.RecalculateConsentRecords,
		manifestReportingClient:      shippingv1.NewManifestReportingServiceClient(deps.Conn),
		shipmentApprovalClient:       shippingv1.NewShipmentApprovalServiceClient(deps.Conn),
		trackingEventReportingClient: shippingv1.NewTrackingEventReportingServiceClient(deps.Conn),
		clients:                      deps.Clients,
		shipmentCalculationClient:    shippingv1.NewShipmentCalculationServiceClient(deps.Conn),
		analyticsEventClient:         analyticsv1.NewAnalyticsEventServiceClient(deps.Conn),
	}
}

// ActivateManifest activates a manifest.
func (s *ManifestLifecycleServer) ActivateManifest(ctx context.Context, req *shippingv1.ActivateManifestRequest) (*shippingv1.ActivateManifestResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "manifest", req.GetManifestId(), "activate_manifest"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate manifest", "tenant", req.GetContext().GetTenantId())
	var holder struct{ DisplayName string }
	if record := req.GetDetails().GetNewOwner(); record != nil {
		if err := mapper.CopyFields(&holder, record, "DisplayName"); err != nil {
			s.log.WarnContext(ctx, "copy display_name", "error", err)
		}
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	calculateShipmentRequest := &shippingv1.CalculateShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			Counterparty: &shippingv1.ParcelRef{DisplayName: holder.DisplayName},
			Audit:        &commonv1.AuditInfo{CreatedAt: req.GetDetails().GetEffectiveAt()},
		},
		Reason:      req.GetReason(),
		Annotations: annotations,
	}
	calculateShipmentReply, err := s.calculateShipmentFn(ctx, calculateShipmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/CalculateShipment")
	}
	delegateGiftCardApprovalRequest := &giftcardsv1.DelegateGiftCardApprovalRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.DelegateGiftCardApprovalRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetDetails().GetNewOwner().GetEmail()},
		},
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: calculateShipmentReply.GetShipment().GetAudit().GetUpdatedAt(),
				UpdatedBy: calculateShipmentReply.GetShipment().GetAudit().GetUpdatedBy(),
			},
		},
	}
	delegateGiftCardApprovalReply, err := s.meteredGiftCardApproval.DelegateGiftCardApproval(ctx, delegateGiftCardApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardApprovalService/DelegateGiftCardApproval")
	}
	return &shippingv1.ActivateManifestResponse{
		Manifest: &shippingv1.Manifest{
			Audit: &commonv1.AuditInfo{
				Etag:      calculateShipmentReply.GetShipment().GetAudit().GetEtag(),
				CreatedBy: delegateGiftCardApprovalReply.GetGiftCard().GetAudit().GetCreatedBy(),
			},
			ManifestId: req.GetManifestId(),
		},
		Outcome: &shippingv1.ActivateManifestResponse_Outcome{
			ReferenceId: delegateGiftCardApprovalReply.GetOutcome().GetReferenceId(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SuspendManifest suspends a manifest.
func (s *ManifestLifecycleServer) SuspendManifest(ctx context.Context, req *shippingv1.SuspendManifestRequest) (*shippingv1.SuspendManifestResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "manifest", req.GetManifestId(), "suspend_manifest"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend manifest", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	listCarrierRateReportsRequest := &shippingv1.ListCarrierRateReportsRequest{
		Context: req.GetContext(),
		Filter: &shippingv1.ListCarrierRateReportsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
			Labels:       annotations,
		},
	}
	listCarrierRateReportsReply, err := s.meteredCarrierRateReporting.ListCarrierRateReports(ctx, listCarrierRateReportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateReportingService/ListCarrierRateReports")
	}
	countCarrierRateTotalsRequest := newCountCarrierRateTotalsRequest(withCountCarrierRateTotalsContext(req.GetContext()), withCountCarrierRateTotalsFilter(&shippingv1.CountCarrierRateTotalsRequest_Filter{
		OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
		CreatedAfter: req.GetDetails().GetEffectiveAt(),
		Labels:       listCarrierRateReportsReply.GetResult().GetDetails(),
	}))
	countCarrierRateTotalsReply, err := s.carrierRateReportingClient.CountCarrierRateTotals(ctx, countCarrierRateTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateReportingService/CountCarrierRateTotals")
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded shippingv1.SuspendManifestRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	listPendingGiftCardsRequest := &giftcardsv1.ListPendingGiftCardsRequest{}
	listPendingGiftCardsRequest.Context = req.GetContext()
	listPendingGiftCardsRequest.Filter = &giftcardsv1.ListPendingGiftCardsRequest_Filter{
		CreatedAfter: (&decoded).GetEffectiveAt(),
		OwnerId:      countCarrierRateTotalsReply.GetResult().GetCorrelationId(),
		Labels:       countCarrierRateTotalsReply.GetResult().GetDetails(),
	}
	listPendingGiftCardsReply, err := s.giftCardApprovalClient.ListPendingGiftCards(ctx, listPendingGiftCardsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardApprovalService/ListPendingGiftCards")
	}
	return &shippingv1.SuspendManifestResponse{
		Result: &commonv1.OperationResult{
			Details:       listCarrierRateReportsReply.GetResult().GetDetails(),
			CorrelationId: countCarrierRateTotalsReply.GetResult().GetCorrelationId(),
			Status:        listPendingGiftCardsReply.GetResult().GetStatus(),
			Message:       listPendingGiftCardsReply.GetResult().GetMessage(),
		},
		Outcome: &shippingv1.SuspendManifestResponse_Outcome{
			Status:      countCarrierRateTotalsReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Manifest: &shippingv1.Manifest{ManifestId: req.GetManifestId()},
	}, nil
}

// ReinstateManifest reinstates a manifest.
func (s *ManifestLifecycleServer) ReinstateManifest(ctx context.Context, req *shippingv1.ReinstateManifestRequest) (*shippingv1.ReinstateManifestResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "manifest", req.GetManifestId(), "reinstate_manifest"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate manifest", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	recalculateConsentRecordsRequest := &customersv1.RecalculateConsentRecordsRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			Detail: packed,
		},
		Reason: req.GetDetails().GetReasonCode(),
	}
	recalculateConsentRecordsReply, err := s.recalculateConsentRecordsFn(ctx, recalculateConsentRecordsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/RecalculateConsentRecords")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	generateManifestReportReply, err := s.manifestReportingClient.GenerateManifestReport(ctx, &shippingv1.GenerateManifestReportRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: annotations,
		ManifestId:  req.GetManifestId(),
		Details: &shippingv1.GenerateManifestReportRequest_Details{
			Filters: recalculateConsentRecordsReply.GetResult().GetDetails(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestReportingService/GenerateManifestReport")
	}
	approveShipmentRequest := &shippingv1.ApproveShipmentRequest{
		Context: req.GetContext(),
		Details: &shippingv1.ApproveShipmentRequest_Details{
			Approver: &identityv1.UserRef{UserId: req.GetDetails().GetNewOwner().GetUserId()},
		},
		Shipment: &shippingv1.Shipment{
			Counterparty: &shippingv1.ParcelRef{
				DisplayName: recalculateConsentRecordsReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
		},
	}
	type approveShipmentResult struct {
		reply *shippingv1.ApproveShipmentResponse
		err   error
	}
	approveShipmentResults := make(chan approveShipmentResult, 1)
	go func() {
		reply, err := s.shipmentApprovalClient.ApproveShipment(ctx, approveShipmentRequest)
		approveShipmentResults <- approveShipmentResult{reply, err}
	}()
	var approveShipmentReply *shippingv1.ApproveShipmentResponse
	select {
	case result := <-approveShipmentResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.shipping.v1.ShipmentApprovalService/ApproveShipment")
		}
		approveShipmentReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &shippingv1.ReinstateManifestResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: recalculateConsentRecordsReply.GetConsentRecord().GetCorrelationId(),
			ReferenceIds:  recalculateConsentRecordsReply.GetResult().GetReferenceIds(),
			Details:       generateManifestReportReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		Outcome: &shippingv1.ReinstateManifestResponse_Outcome{
			Status:      approveShipmentReply.GetOutcome().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Manifest: &shippingv1.Manifest{
			Total:      approveShipmentReply.GetShipment().GetTotal(),
			ManifestId: req.GetManifestId(),
		},
	}, nil
}

// ExpireManifest expires a manifest.
func (s *ManifestLifecycleServer) ExpireManifest(ctx context.Context, req *shippingv1.ExpireManifestRequest) (*shippingv1.ExpireManifestResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "manifest", req.GetManifestId(), "expire_manifest"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire manifest", "tenant", req.GetContext().GetTenantId())
	scheduleTrackingEventReportRequest := &shippingv1.ScheduleTrackingEventReportRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
	}
	type scheduleTrackingEventReportResult struct {
		reply *shippingv1.ScheduleTrackingEventReportResponse
		err   error
	}
	scheduleTrackingEventReportResults := make(chan scheduleTrackingEventReportResult, 1)
	go func() {
		reply, err := s.trackingEventReportingClient.ScheduleTrackingEventReport(ctx, scheduleTrackingEventReportRequest)
		scheduleTrackingEventReportResults <- scheduleTrackingEventReportResult{reply, err}
	}()
	var scheduleTrackingEventReportReply *shippingv1.ScheduleTrackingEventReportResponse
	select {
	case result := <-scheduleTrackingEventReportResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.shipping.v1.TrackingEventReportingService/ScheduleTrackingEventReport")
		}
		scheduleTrackingEventReportReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	jobOpeningQuery, ok := s.clients.Get("acme.recruiting.v1.JobOpeningQueryService").(recruitingv1.JobOpeningQueryServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.recruiting.v1.JobOpeningQueryService")
	}
	resolveJobOpeningsRequest := &recruitingv1.ResolveJobOpeningsRequest{Context: req.GetContext()}
	resolveJobOpeningsReply, err := jobOpeningQuery.ResolveJobOpenings(ctx, resolveJobOpeningsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.recruiting.v1.JobOpeningQueryService/ResolveJobOpenings")
	}
	return &shippingv1.ExpireManifestResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: scheduleTrackingEventReportReply.GetResult().GetReferenceIds(),
			Status:       resolveJobOpeningsReply.GetResult().GetStatus(),
			Message:      resolveJobOpeningsReply.GetResult().GetMessage(),
		},
		Manifest: &shippingv1.Manifest{ManifestId: req.GetManifestId()},
		Outcome:  &shippingv1.ExpireManifestResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// GetManifestLifecycle returns a manifest.
func (s *ManifestLifecycleServer) GetManifestLifecycle(ctx context.Context, req *shippingv1.GetManifestLifecycleRequest) (*shippingv1.GetManifestLifecycleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "manifest", req.GetManifestId(), "get_manifest_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get manifest lifecycle", "tenant", req.GetContext().GetTenantId())
	return &shippingv1.GetManifestLifecycleResponse{
		Manifest: &shippingv1.Manifest{ManifestId: req.GetManifestId()},
		Result:   &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScheduleManifestExpiry schedules a manifest.
func (s *ManifestLifecycleServer) ScheduleManifestExpiry(ctx context.Context, req *shippingv1.ScheduleManifestExpiryRequest) (*shippingv1.ScheduleManifestExpiryResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveManifest(ctx, req.GetContext().GetTenantId(), req.GetManifest()); err != nil {
		return nil, grpcx.Wrap(err, "save manifest")
	}
	s.log.InfoContext(ctx, "schedule manifest expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetManifest().GetAudit().GetEtag())
	var token string
	switch choice := req.GetManifest().GetSettlement().(type) {
	case *shippingv1.Manifest_Card:
		token = choice.Card.GetToken()
	case *shippingv1.Manifest_Bank:
		token = choice.Bank.GetIban()
	case *shippingv1.Manifest_VoucherCode:
		token = choice.VoucherCode
	}
	var holder struct{ CreatedBy string }
	if record := req.GetManifest().GetAudit(); record != nil {
		if err := mapper.CopyFields(&holder, record, "CreatedBy"); err != nil {
			s.log.WarnContext(ctx, "copy created_by", "error", err)
		}
	}
	step := &manifestLifecycleServerScheduleManifestExpirySimulateShipmentStep{request: &shippingv1.SimulateShipmentRequest{
		Context: req.GetContext(),
		Details: &shippingv1.SimulateShipmentRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: req.GetManifest().GetBillingAddress().GetCountryCode(),
			},
		},
		Shipment: &shippingv1.Shipment{
			Settlement: &shippingv1.Shipment_Card{Card: &shippingv1.ShipmentCardSettlement{
				HolderName: req.GetManifest().GetCard().GetHolderName(),
				Token:      token,
			}},
			Audit: &commonv1.AuditInfo{CreatedBy: holder.CreatedBy},
		},
	}}
	if err := s.runScheduleManifestExpirySimulateShipment(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/SimulateShipment")
	}
	simulateShipmentReply := step.reply
	listAnalyticsEventsRequest := &analyticsv1.ListAnalyticsEventsRequest{
		Context: req.GetContext(),
		Filter: &analyticsv1.ListAnalyticsEventsRequest_Filter{
			Region:       simulateShipmentReply.GetShipment().GetBillingAddress().GetRegion(),
			CreatedAfter: simulateShipmentReply.GetShipment().GetAudit().GetCreatedAt(),
		},
	}
	type listAnalyticsEventsResult struct {
		reply *analyticsv1.ListAnalyticsEventsResponse
		err   error
	}
	listAnalyticsEventsResults := make(chan listAnalyticsEventsResult, 1)
	go func() {
		reply, err := s.analyticsEventClient.ListAnalyticsEvents(ctx, listAnalyticsEventsRequest)
		listAnalyticsEventsResults <- listAnalyticsEventsResult{reply, err}
	}()
	var listAnalyticsEventsReply *analyticsv1.ListAnalyticsEventsResponse
	select {
	case result := <-listAnalyticsEventsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.analytics.v1.AnalyticsEventService/ListAnalyticsEvents")
		}
		listAnalyticsEventsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &shippingv1.ScheduleManifestExpiryResponse{
		Outcome: &shippingv1.ScheduleManifestExpiryResponse_Outcome{Status: simulateShipmentReply.GetResult().GetStatus()},
		Manifest: &shippingv1.Manifest{
			Audit: &commonv1.AuditInfo{
				CreatedBy: simulateShipmentReply.GetShipment().GetAudit().GetCreatedBy(),
			},
			ManifestId:     req.GetManifestId(),
			BillingAddress: req.GetManifest().GetBillingAddress(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: listAnalyticsEventsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// manifestLifecycleServerScheduleManifestExpirySimulateShipmentStep carries the request of one step of
// ScheduleManifestExpiry and, once it ran, its reply.
type manifestLifecycleServerScheduleManifestExpirySimulateShipmentStep struct {
	request *shippingv1.SimulateShipmentRequest
	reply   *shippingv1.SimulateShipmentResponse
}

func (s *ManifestLifecycleServer) runScheduleManifestExpirySimulateShipment(ctx context.Context, step *manifestLifecycleServerScheduleManifestExpirySimulateShipmentStep) error {
	reply, err := s.shipmentCalculationClient.SimulateShipment(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
