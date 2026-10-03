package shipping

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	broadcastsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/broadcasts/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	deliveriesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/deliveries/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// ParcelApprovalServer implements `acme.shipping.v1.ParcelApprovalService`.
// Routes parcels through approval, with escalation and delegation.
type ParcelApprovalServer struct {
	shippingv1.UnimplementedParcelApprovalServiceServer
	store                           *Store
	log                             *slog.Logger
	conn                            *grpc.ClientConn
	consentRecordValidationClient   customersv1.ConsentRecordValidationServiceClient
	clients                         *registry.Registry
	sessionQueryClient              identityv1.SessionQueryServiceClient
	notificationQueryClient         notificationsv1.NotificationQueryServiceClient
	sellerListingApprovalClient     sellersv1.SellerListingApprovalServiceClient
	manifestLifecycleClient         shippingv1.ManifestLifecycleServiceClient
	customerSegmentValidationClient customersv1.CustomerSegmentValidationServiceClient
	carrierRateReportingClient      shippingv1.CarrierRateReportingServiceClient
	deliveryClient                  deliveriesv1.DeliveryServiceClient
	bus                             *eventbus.Bus
	shipmentApprovalClient          shippingv1.ShipmentApprovalServiceClient
	parcelLifecycleClient           shippingv1.ParcelLifecycleServiceClient
	trackingEventCalculationClient  shippingv1.TrackingEventCalculationServiceClient
	tenantAdminClient               identityv1.TenantAdminServiceClient
}

// NewParcelApprovalServer wires a ParcelApprovalServer.
func NewParcelApprovalServer(deps platform.Deps) *ParcelApprovalServer {
	return &ParcelApprovalServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.shipping.v1.ParcelApprovalService"),
		conn:                            deps.Conn,
		consentRecordValidationClient:   customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		clients:                         deps.Clients,
		sessionQueryClient:              identityv1.NewSessionQueryServiceClient(deps.Conn),
		notificationQueryClient:         notificationsv1.NewNotificationQueryServiceClient(deps.Conn),
		sellerListingApprovalClient:     sellersv1.NewSellerListingApprovalServiceClient(deps.Conn),
		manifestLifecycleClient:         shippingv1.NewManifestLifecycleServiceClient(deps.Conn),
		customerSegmentValidationClient: customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn),
		carrierRateReportingClient:      shippingv1.NewCarrierRateReportingServiceClient(deps.Conn),
		deliveryClient:                  deliveriesv1.NewDeliveryServiceClient(deps.Conn),
		bus:                             deps.Bus,
		shipmentApprovalClient:          shippingv1.NewShipmentApprovalServiceClient(deps.Conn),
		parcelLifecycleClient:           shippingv1.NewParcelLifecycleServiceClient(deps.Conn),
		trackingEventCalculationClient:  shippingv1.NewTrackingEventCalculationServiceClient(deps.Conn),
		tenantAdminClient:               identityv1.NewTenantAdminServiceClient(deps.Conn),
	}
}

// SubmitParcel submits a parcel.
func (s *ParcelApprovalServer) SubmitParcel(ctx context.Context, req *shippingv1.SubmitParcelRequest) (*shippingv1.SubmitParcelResponse, error) {
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "submit parcel", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetParcel())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded shippingv1.Parcel
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	delegateSubscriptionPlanApprovalRequest := &subscriptionsv1.DelegateSubscriptionPlanApprovalRequest{
		Context: req.GetContext(),
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Owner: &identityv1.UserRef{Email: (&decoded).GetOwner().GetEmail()},
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetParcel().GetAudit().GetUpdatedBy()},
		},
		Details: &subscriptionsv1.DelegateSubscriptionPlanApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
	}
	delegateSubscriptionPlanApprovalReply := new(subscriptionsv1.DelegateSubscriptionPlanApprovalResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", subscriptionsSubscriptionPlanApprovalServiceName, "DelegateSubscriptionPlanApproval"), delegateSubscriptionPlanApprovalRequest, delegateSubscriptionPlanApprovalReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanApprovalService/DelegateSubscriptionPlanApproval")
	}
	var region string
	switch choice := req.GetParcel().GetLocator().(type) {
	case *shippingv1.Parcel_Address:
		region = choice.Address.GetRegion()
	case *shippingv1.Parcel_FacilityCode:
		region = choice.FacilityCode
	}
	scoreCustomerRequest := &customersv1.ScoreCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Audit:               &commonv1.AuditInfo{CreatedAt: req.GetParcel().GetAudit().GetCreatedAt()},
			Contact:             &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
			RelationshipManager: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
		},
		Details: &customersv1.ScoreCustomerRequest_Details{Destination: &commonv1.PostalAddress{Region: region}},
	}
	scoreCustomerReply, err := scoreCustomerCall(ctx, scoreCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/ScoreCustomer")
	}
	return &shippingv1.SubmitParcelResponse{
		Result: &commonv1.OperationResult{
			Details: delegateSubscriptionPlanApprovalReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Parcel: &shippingv1.Parcel{
			Audit:    &commonv1.AuditInfo{Etag: scoreCustomerReply.GetCustomer().GetAudit().GetEtag()},
			ParcelId: req.GetParcelId(),
		},
		Outcome: &shippingv1.SubmitParcelResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ApproveParcel approves a parcel.
func (s *ParcelApprovalServer) ApproveParcel(ctx context.Context, req *shippingv1.ApproveParcelRequest) (*shippingv1.ApproveParcelResponse, error) {
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "approve parcel", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetParcel())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainConsentRecordDecisionRequest := newExplainConsentRecordDecisionRequest(withExplainConsentRecordDecisionContext(req.GetContext()), withExplainConsentRecordDecisionConsentRecord(&customersv1.ConsentRecord{
		Subject: &customersv1.HouseholdRef{
			DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
		},
		Detail: packed,
	}), withExplainConsentRecordDecisionDetails(&customersv1.ExplainConsentRecordDecisionRequest_Details{
		Contact: &commonv1.ContactInfo{Email: req.GetParcel().GetOwner().GetEmail()},
	}))
	explainConsentRecordDecisionReply, err := s.consentRecordValidationClient.ExplainConsentRecordDecision(ctx, explainConsentRecordDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ExplainConsentRecordDecision")
	}
	manifestCalculation, ok := s.clients.Get("acme.shipping.v1.ManifestCalculationService").(shippingv1.ManifestCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.shipping.v1.ManifestCalculationService")
	}
	compareManifestOptionsRequest := &shippingv1.CompareManifestOptionsRequest{
		Context: req.GetContext(),
		Manifest: &shippingv1.Manifest{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetParcel().GetAudit().GetCreatedAt()},
		},
	}
	compareManifestOptionsReply, err := manifestCalculation.CompareManifestOptions(ctx, compareManifestOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestCalculationService/CompareManifestOptions")
	}
	return &shippingv1.ApproveParcelResponse{
		Outcome: &shippingv1.ApproveParcelResponse_Outcome{
			Status:  explainConsentRecordDecisionReply.GetResult().GetStatus(),
			Message: explainConsentRecordDecisionReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			Message: compareManifestOptionsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Parcel: &shippingv1.Parcel{
			ParcelId: req.GetParcelId(),
			Specs:    req.GetParcel().GetSpecs(),
		},
	}, nil
}

// RejectParcel rejects a parcel.
func (s *ParcelApprovalServer) RejectParcel(ctx context.Context, req *shippingv1.RejectParcelRequest) (*shippingv1.RejectParcelResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RejectParcel", "error", err)
		return nil, err
	}
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "reject parcel", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	exportSessionsStream, err := s.sessionQueryClient.ExportSessions(ctx, &identityv1.ExportSessionsRequest{
		Context: req.GetContext(),
		Filter:  &identityv1.ExportSessionsRequest_Filter{Region: req.GetParcel().GetAddress().GetRegion()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/ExportSessions")
	}
	for {
		received, err := exportSessionsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/ExportSessions")
		}
		_ = received
	}
	var region string
	switch choice := req.GetParcel().GetLocator().(type) {
	case *shippingv1.Parcel_Address:
		region = choice.Address.GetRegion()
	case *shippingv1.Parcel_FacilityCode:
		region = choice.FacilityCode
	}
	exportNotificationsStream, err := s.notificationQueryClient.ExportNotifications(ctx, &notificationsv1.ExportNotificationsRequest{
		Context: req.GetContext(),
		Filter:  &notificationsv1.ExportNotificationsRequest_Filter{Region: region},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationQueryService/ExportNotifications")
	}
	for {
		received2, err := exportNotificationsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationQueryService/ExportNotifications")
		}
		_ = received2
	}
	return &shippingv1.RejectParcelResponse{
		Parcel: &shippingv1.Parcel{
			ParcelId: req.GetParcelId(),
			Owner:    req.GetParcel().GetOwner(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EscalateParcel escalates a parcel.
func (s *ParcelApprovalServer) EscalateParcel(ctx context.Context, req *shippingv1.EscalateParcelRequest) (*shippingv1.EscalateParcelResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EscalateParcel", "error", err)
		return nil, err
	}
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "escalate parcel", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	rejectSellerListingRequest := &sellersv1.RejectSellerListingRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetParcel().GetAudit().GetCreatedAt()},
			Owner: &identityv1.UserRef{UserId: req.GetParcel().GetOwner().GetUserId()},
		},
	}
	rejectSellerListingReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*sellersv1.RejectSellerListingResponse, error) {
		return s.sellerListingApprovalClient.RejectSellerListing(ctx, rejectSellerListingRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingApprovalService/RejectSellerListing")
	}
	scheduleManifestExpiryRequest := &shippingv1.ScheduleManifestExpiryRequest{
		Context: req.GetContext(),
		Manifest: &shippingv1.Manifest{
			Audit: &commonv1.AuditInfo{
				CreatedBy: rejectSellerListingReply.GetSellerListing().GetAudit().GetCreatedBy(),
			},
			Counterparty: &customersv1.CustomerRef{
				Email: rejectSellerListingReply.GetSellerListing().GetOwner().GetEmail(),
			},
		},
	}
	scheduleManifestExpiryReply, err := shippingv1.ManifestLifecycleServiceClient.ScheduleManifestExpiry(s.manifestLifecycleClient, ctx, scheduleManifestExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestLifecycleService/ScheduleManifestExpiry")
	}
	previewCustomerSegmentReply, err := s.customerSegmentValidationClient.PreviewCustomerSegment(ctx, &customersv1.PreviewCustomerSegmentRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{
				CreatedAt: rejectSellerListingReply.GetSellerListing().GetAudit().GetCreatedAt(),
				UpdatedAt: scheduleManifestExpiryReply.GetManifest().GetAudit().GetUpdatedAt(),
			},
		},
		Details: &customersv1.PreviewCustomerSegmentRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: rejectSellerListingReply.GetSellerListing().GetOwner().GetEmail(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/PreviewCustomerSegment")
	}
	return &shippingv1.EscalateParcelResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: rejectSellerListingReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Parcel: &shippingv1.Parcel{
			Dimensions: &shippingv1.ParcelDimensions{
				Length: rejectSellerListingReply.GetSellerListing().GetDimensions().GetLength(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: scheduleManifestExpiryReply.GetManifest().GetAudit().GetCreatedBy(),
				CreatedAt: scheduleManifestExpiryReply.GetManifest().GetAudit().GetCreatedAt(),
			},
			ParcelId: req.GetParcelId(),
			Owner:    req.GetParcel().GetOwner(),
		},
		Outcome: &shippingv1.EscalateParcelResponse_Outcome{
			Status: previewCustomerSegmentReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// ListPendingParcels lists parcels a page at a time.
func (s *ParcelApprovalServer) ListPendingParcels(ctx context.Context, req *shippingv1.ListPendingParcelsRequest) (*shippingv1.ListPendingParcelsResponse, error) {
	s.log.InfoContext(ctx, "list pending parcels", "tenant", req.GetContext().GetTenantId())
	streamCarrierRateMetricsStream, err := s.carrierRateReportingClient.StreamCarrierRateMetrics(ctx, &shippingv1.StreamCarrierRateMetricsRequest{
		Context: req.GetContext(),
		Filter:  &shippingv1.StreamCarrierRateMetricsRequest_Filter{CreatedAfter: req.GetFilter().GetCreatedAfter()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateReportingService/StreamCarrierRateMetrics")
	}
	for {
		received, err := streamCarrierRateMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateReportingService/StreamCarrierRateMetrics")
		}
		_ = received
	}
	return &shippingv1.ListPendingParcelsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// DelegateParcelApproval delegates a parcel.
func (s *ParcelApprovalServer) DelegateParcelApproval(ctx context.Context, req *shippingv1.DelegateParcelApprovalRequest) (*shippingv1.DelegateParcelApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "delegate parcel approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	watchDeliveriesStream, err := s.deliveryClient.WatchDeliveries(ctx, &deliveriesv1.WatchDeliveriesRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetParcel().GetOwner().GetEmail()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.deliveries.v1.DeliveryService/WatchDeliveries")
	}
	for {
		received, err := watchDeliveriesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.deliveries.v1.DeliveryService/WatchDeliveries")
		}
		_ = received
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "shipping.delegate_parcel_approval.recalculate_households",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"household.children.children.manager.display_name": req.GetParcel().GetOwner().GetDisplayName(),
			"household.children.children.audit.created_by":     req.GetParcel().GetAudit().GetCreatedBy(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &shippingv1.DelegateParcelApprovalResponse{
		Parcel: &shippingv1.Parcel{
			ParcelId: req.GetParcelId(),
			Specs:    req.GetParcel().GetSpecs(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecallParcel recalls a parcel.
func (s *ParcelApprovalServer) RecallParcel(ctx context.Context, req *shippingv1.RecallParcelRequest) (*shippingv1.RecallParcelResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "recall parcel", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	createBroadcastRequest := &broadcastsv1.CreateBroadcastRequest{
		Context: req.GetContext(),
		Broadcast: &broadcastsv1.Broadcast{
			Participants: []*broadcastsv1.BroadcastParticipant{&broadcastsv1.BroadcastParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
			}},
		},
	}
	createBroadcastReply, err := grpcx.NewClient(s.conn, broadcastsv1.NewBroadcastServiceClient).CreateBroadcast(ctx, createBroadcastRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.broadcasts.v1.BroadcastService/CreateBroadcast")
	}
	return &shippingv1.RecallParcelResponse{
		Outcome: &shippingv1.RecallParcelResponse_Outcome{Message: createBroadcastReply.GetResult().GetMessage()},
		Parcel: &shippingv1.Parcel{
			ParcelId: req.GetParcelId(),
			Status:   req.GetParcel().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetParcelApprovalStatus returns a parcel.
func (s *ParcelApprovalServer) GetParcelApprovalStatus(ctx context.Context, req *shippingv1.GetParcelApprovalStatusRequest) (*shippingv1.GetParcelApprovalStatusResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "parcel", req.GetParcelId(), "get_parcel_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get parcel approval status", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "parcel_id", req.GetParcelId())
	source := req.GetContext().GetActor().ProtoReflect()
	userId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("user_id"))).String()
	submitShipmentRequest := &shippingv1.SubmitShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			Counterparty: &shippingv1.ParcelRef{ParcelId: tenancy.Value(ctx, "parcel_id")},
			Audit:        &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
		},
		Details: &shippingv1.SubmitShipmentRequest_Details{
			Approver: &identityv1.UserRef{
				UserId: userId,
				Email:  req.GetContext().GetActor().GetEmail(),
			},
		},
	}
	type submitShipmentResult struct {
		reply *shippingv1.SubmitShipmentResponse
		err   error
	}
	submitShipmentResults := make(chan submitShipmentResult, 1)
	go func() {
		reply, err := s.shipmentApprovalClient.SubmitShipment(ctx, submitShipmentRequest)
		submitShipmentResults <- submitShipmentResult{reply, err}
	}()
	var submitShipmentReply *shippingv1.SubmitShipmentResponse
	select {
	case result := <-submitShipmentResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.shipping.v1.ShipmentApprovalService/SubmitShipment")
		}
		submitShipmentReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &shippingv1.GetParcelApprovalStatusResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: submitShipmentReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Parcel: &shippingv1.Parcel{ParcelId: req.GetParcelId()},
	}, nil
}

// CommentParcelApproval comments a parcel.
func (s *ParcelApprovalServer) CommentParcelApproval(ctx context.Context, req *shippingv1.CommentParcelApprovalRequest) (*shippingv1.CommentParcelApprovalResponse, error) {
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "comment parcel approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	reinstateParcelReply, err := s.parcelLifecycleClient.ReinstateParcel(ctx, &shippingv1.ReinstateParcelRequest{
		Context:  req.GetContext(),
		ParcelId: req.GetParcel().GetParcelId(),
		Details: &shippingv1.ReinstateParcelRequest_Details{
			NewOwner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			NotifyContact: &commonv1.ContactInfo{Email: req.GetParcel().GetOwner().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelLifecycleService/ReinstateParcel")
	}
	recalculateTrackingEventsRequest := buildRecalculateTrackingEventsRequestForCommentParcelApproval(req)
	recalculateTrackingEventsReply, err := s.trackingEventCalculationClient.RecalculateTrackingEvents(ctx, recalculateTrackingEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventCalculationService/RecalculateTrackingEvents")
	}
	ctx = tenancy.With(ctx, "tenant_id", req.GetContext().GetTenantId())
	parcelCache := cache.New[string, *shippingv1.Parcel](1)
	parcelCache.Put(req.GetContext().GetTraceId(), req.GetParcel())
	cachedParcel, _ := parcelCache.Get(req.GetContext().GetTraceId())
	lockTenantRequest := &identityv1.LockTenantRequest{
		Context:  req.GetContext(),
		TenantId: tenancy.Value(ctx, "tenant_id"),
		Details: &identityv1.LockTenantRequest_Details{
			NewOwner: &identityv1.UserRef{
				DisplayName: reinstateParcelReply.GetParcel().GetOwner().GetDisplayName(),
				Email:       cachedParcel.GetOwner().GetEmail(),
			},
			NotifyContact: &commonv1.ContactInfo{
				Email: reinstateParcelReply.GetParcel().GetOwner().GetEmail(),
			},
		},
	}
	lockTenantReply, err := grpcx.Call(ctx, s.tenantAdminClient.LockTenant, lockTenantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/LockTenant")
	}
	return &shippingv1.CommentParcelApprovalResponse{
		Outcome: &shippingv1.CommentParcelApprovalResponse_Outcome{
			Status:      reinstateParcelReply.GetOutcome().GetStatus(),
			ReferenceId: reinstateParcelReply.GetOutcome().GetReferenceId(),
		},
		Parcel: &shippingv1.Parcel{
			Owner: &identityv1.UserRef{
				DisplayName: recalculateTrackingEventsReply.GetTrackingEvent().GetSubject().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: lockTenantReply.GetTenant().GetAudit().GetCreatedAt(),
			},
			ParcelId: req.GetParcelId(),
			Name:     req.GetParcel().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
