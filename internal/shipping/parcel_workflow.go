package shipping

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ParcelWorkflowServer implements `acme.shipping.v1.ParcelWorkflowService`.
// Runs the long-lived workflow that takes a parcel from start to finish.
type ParcelWorkflowServer struct {
	*parcelWorkflowHandlers
}

// parcelWorkflowHandlers holds the handlers of ParcelWorkflowServer, with what they use.
type parcelWorkflowHandlers struct {
	shippingv1.UnimplementedParcelWorkflowServiceServer
	store                           *Store
	log                             *slog.Logger
	manifestReportingPort           manifestReportingPort
	signalSellerWorkflowFn          func(context.Context, *sellersv1.SignalSellerWorkflowRequest, ...grpc.CallOption) (*sellersv1.SignalSellerWorkflowResponse, error)
	manifestReportingClient         shippingv1.ManifestReportingServiceClient
	customerSegmentValidationClient customersv1.CustomerSegmentValidationServiceClient
	documentClient                  documentsv1.DocumentServiceClient
	conn                            *grpc.ClientConn
	subscriptionWorkflowClient      subscriptionsv1.SubscriptionWorkflowServiceClient
	manifestSyncClient              shippingv1.ManifestSyncServiceClient
	trackingEventReportingClient    shippingv1.TrackingEventReportingServiceClient
	giftCardWorkflowClient          giftcardsv1.GiftCardWorkflowServiceClient
	shipmentApprovalPort            shipmentApprovalPort
	sellerScorecardSyncClient       sellersv1.SellerScorecardSyncServiceClient
	notificationValidationClient    notificationsv1.NotificationValidationServiceClient
	carrierRateApprovalPort         carrierRateApprovalPort
}

// NewParcelWorkflowServer wires a ParcelWorkflowServer.
func NewParcelWorkflowServer(deps platform.Deps) *ParcelWorkflowServer {
	sellerWorkflow := sellersv1.NewSellerWorkflowServiceClient(deps.Conn)
	return &ParcelWorkflowServer{parcelWorkflowHandlers: &parcelWorkflowHandlers{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.shipping.v1.ParcelWorkflowService"),
		manifestReportingPort:           newManifestReportingPort(deps),
		signalSellerWorkflowFn:          sellerWorkflow.SignalSellerWorkflow,
		manifestReportingClient:         shippingv1.NewManifestReportingServiceClient(deps.Conn),
		customerSegmentValidationClient: customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn),
		documentClient:                  documentsv1.NewDocumentServiceClient(deps.Conn),
		conn:                            deps.Conn,
		subscriptionWorkflowClient:      subscriptionsv1.NewSubscriptionWorkflowServiceClient(deps.Conn),
		manifestSyncClient:              shippingv1.NewManifestSyncServiceClient(deps.Conn),
		trackingEventReportingClient:    shippingv1.NewTrackingEventReportingServiceClient(deps.Conn),
		giftCardWorkflowClient:          giftcardsv1.NewGiftCardWorkflowServiceClient(deps.Conn),
		shipmentApprovalPort:            newShipmentApprovalPort(deps),
		sellerScorecardSyncClient:       sellersv1.NewSellerScorecardSyncServiceClient(deps.Conn),
		notificationValidationClient:    notificationsv1.NewNotificationValidationServiceClient(deps.Conn),
		carrierRateApprovalPort:         newCarrierRateApprovalPort(deps),
	}}
}

// StartParcelWorkflow starts a parcel.
func (s *parcelWorkflowHandlers) StartParcelWorkflow(ctx context.Context, req *shippingv1.StartParcelWorkflowRequest) (*shippingv1.StartParcelWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected StartParcelWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "start parcel workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	listManifestReportsRequest := &shippingv1.ListManifestReportsRequest{
		Context: req.GetContext(),
		Filter: &shippingv1.ListManifestReportsRequest_Filter{
			Region:       req.GetParcel().GetAddress().GetRegion(),
			CreatedAfter: req.GetParcel().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetParcel().GetComponents()["primary"].GetComponentId(),
		},
	}
	listManifestReportsReply, err := s.manifestReportingPort.ListManifestReports(ctx, listManifestReportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestReportingService/ListManifestReports")
	}
	return &shippingv1.StartParcelWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message: listManifestReportsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &shippingv1.StartParcelWorkflowResponse_Outcome{Message: listManifestReportsReply.GetResult().GetMessage()},
		Parcel: &shippingv1.Parcel{
			ParcelId: req.GetParcelId(),
			Capacity: req.GetParcel().GetCapacity(),
		},
	}, nil
}

// AdvanceParcelWorkflow advances a parcel.
func (s *parcelWorkflowHandlers) AdvanceParcelWorkflow(ctx context.Context, req *shippingv1.AdvanceParcelWorkflowRequest) (*shippingv1.AdvanceParcelWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "advance parcel workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "phone", req.GetDetails().GetNotify().GetPhone())
	signalSellerWorkflowRequest := &sellersv1.SignalSellerWorkflowRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			Audit: &commonv1.AuditInfo{Etag: req.GetParcel().GetAudit().GetEtag()},
			PrimaryAddress: &commonv1.PostalAddress{
				Recipient:   req.GetParcel().GetAddress().GetRecipient(),
				CountryCode: req.GetParcel().GetAddress().GetCountryCode(),
			},
		},
		Details: &sellersv1.SignalSellerWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Phone: tenancy.Value(ctx, "phone")},
		},
	}
	signalSellerWorkflowReply, err := s.signalSellerWorkflowFn(ctx, signalSellerWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerWorkflowService/SignalSellerWorkflow")
	}
	var region string
	switch choice := req.GetParcel().GetLocator().(type) {
	case *shippingv1.Parcel_Address:
		region = choice.Address.GetRegion()
	case *shippingv1.Parcel_FacilityCode:
		region = choice.FacilityCode
	}
	specs := make(map[string]string, len(req.GetParcel().GetSpecs()))
	for key, value := range req.GetParcel().GetSpecs() {
		specs[key] = value
	}
	countManifestTotalsReply, err := s.manifestReportingClient.CountManifestTotals(ctx, &shippingv1.CountManifestTotalsRequest{
		Context: req.GetContext(),
		Filter: &shippingv1.CountManifestTotalsRequest_Filter{
			Region:       region,
			CreatedAfter: signalSellerWorkflowReply.GetSeller().GetPassport().GetExpiresAt(),
			Labels:       specs,
			OwnerId:      signalSellerWorkflowReply.GetOutcome().GetReferenceId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestReportingService/CountManifestTotals")
	}
	validateCustomerSegmentRequest := buildValidateCustomerSegmentRequestForAdvanceParcelWorkflow(req)
	validateCustomerSegmentReply, err := s.customerSegmentValidationClient.ValidateCustomerSegment(ctx, validateCustomerSegmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/ValidateCustomerSegment")
	}
	return &shippingv1.AdvanceParcelWorkflowResponse{
		Outcome: &shippingv1.AdvanceParcelWorkflowResponse_Outcome{
			NextSteps: signalSellerWorkflowReply.GetOutcome().GetNextSteps(),
			Status:    countManifestTotalsReply.GetResult().GetStatus(),
		},
		Parcel: &shippingv1.Parcel{
			Owner: &identityv1.UserRef{
				DisplayName: signalSellerWorkflowReply.GetSeller().GetRelationshipManager().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: validateCustomerSegmentReply.GetCustomerSegment().GetAudit().GetCreatedBy(),
				UpdatedBy: validateCustomerSegmentReply.GetCustomerSegment().GetAudit().GetUpdatedBy(),
			},
			ParcelId: req.GetParcelId(),
			Capacity: req.GetParcel().GetCapacity(),
		},
		Result: &commonv1.OperationResult{
			Message: countManifestTotalsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// CancelParcelWorkflow cancels a parcel.
func (s *parcelWorkflowHandlers) CancelParcelWorkflow(ctx context.Context, req *shippingv1.CancelParcelWorkflowRequest) (*shippingv1.CancelParcelWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "cancel parcel workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	createDocumentRequest := &documentsv1.CreateDocumentRequest{
		Context: req.GetContext(),
		Document: &documentsv1.Document{
			BillingAddress: &commonv1.PostalAddress{Locality: req.GetParcel().GetAddress().GetLocality()},
			Lines: []*documentsv1.DocumentLine{&documentsv1.DocumentLine{
				Quantity: req.GetParcel().GetComponents()["primary"].GetQuantity(),
			}},
		},
	}
	createDocumentReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*documentsv1.CreateDocumentResponse, error) {
		return s.documentClient.CreateDocument(ctx, createDocumentRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentService/CreateDocument")
	}
	comparePreferenceOptionsRequest := &customersv1.ComparePreferenceOptionsRequest{
		Context: req.GetContext(),
		Details: &customersv1.ComparePreferenceOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: createDocumentReply.GetDocument().GetBillingAddress().GetPostalCode(),
			},
		},
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{
				CreatedBy: createDocumentReply.GetDocument().GetAudit().GetCreatedBy(),
				CreatedAt: req.GetParcel().GetAudit().GetCreatedAt(),
			},
		},
	}
	comparePreferenceOptionsReply, err := grpcx.NewClient(s.conn, customersv1.NewPreferenceCalculationServiceClient).ComparePreferenceOptions(ctx, comparePreferenceOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/ComparePreferenceOptions")
	}
	return &shippingv1.CancelParcelWorkflowResponse{
		Parcel: &shippingv1.Parcel{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: createDocumentReply.GetDocument().GetAudit().GetUpdatedAt(),
				CreatedAt: comparePreferenceOptionsReply.GetPreference().GetAudit().GetCreatedAt(),
				CreatedBy: comparePreferenceOptionsReply.GetPreference().GetAudit().GetCreatedBy(),
			},
			ParcelId: req.GetParcelId(),
			Specs:    req.GetParcel().GetSpecs(),
		},
		Result: &commonv1.OperationResult{
			Message: createDocumentReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// RetryParcelWorkflow retrys a parcel.
func (s *parcelWorkflowHandlers) RetryParcelWorkflow(ctx context.Context, req *shippingv1.RetryParcelWorkflowRequest) (*shippingv1.RetryParcelWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "retry parcel workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	retrySubscriptionWorkflowRequest := &subscriptionsv1.RetrySubscriptionWorkflowRequest{
		Context: req.GetContext(),
		Subscription: &subscriptionsv1.Subscription{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetParcel().GetAudit().GetUpdatedAt(),
				CreatedBy: req.GetParcel().GetAudit().GetCreatedBy(),
				UpdatedBy: req.GetParcel().GetAudit().GetUpdatedBy(),
			},
		},
		Details: &subscriptionsv1.RetrySubscriptionWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotify().GetPhone()},
		},
	}
	retrySubscriptionWorkflowReply, err := grpcx.Call(ctx, s.subscriptionWorkflowClient.RetrySubscriptionWorkflow, retrySubscriptionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionWorkflowService/RetrySubscriptionWorkflow")
	}
	checkCustomerSegmentEligibilityRequest := &customersv1.CheckCustomerSegmentEligibilityRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				Email: retrySubscriptionWorkflowReply.GetSubscription().GetContact().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: retrySubscriptionWorkflowReply.GetSubscription().GetAudit().GetCreatedBy(),
				Etag:      req.GetParcel().GetAudit().GetEtag(),
			},
		},
	}
	checkCustomerSegmentEligibilityReply, err := grpcx.Call(ctx, s.customerSegmentValidationClient.CheckCustomerSegmentEligibility, checkCustomerSegmentEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/CheckCustomerSegmentEligibility")
	}
	source := req.GetDetails().GetNotify().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	recalculateCustomersRequest := &customersv1.RecalculateCustomersRequest{
		Context:  req.GetContext(),
		Customer: &customersv1.Customer{RelationshipManager: &identityv1.UserRef{Email: email}},
	}
	recalculateCustomersReply, err := recalculateCustomersCall(ctx, recalculateCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/RecalculateCustomers")
	}
	snapshot, err := protojson.Marshal(req.GetParcel())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pullManifestsRequest := &shippingv1.PullManifestsRequest{}
	pullManifestsRequest.Context = req.GetContext()
	pullManifestsRequest.Manifest = &shippingv1.Manifest{
		BillingAddress: &commonv1.PostalAddress{
			CountryCode: recalculateCustomersReply.GetCustomer().GetPrimaryAddress().GetCountryCode(),
		},
	}
	pullManifestsRequest.Details = &shippingv1.PullManifestsRequest_Details{Payload: snapshot}
	pullManifestsReply, err := s.manifestSyncClient.PullManifests(ctx, pullManifestsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestSyncService/PullManifests")
	}
	return &shippingv1.RetryParcelWorkflowResponse{
		Outcome: &shippingv1.RetryParcelWorkflowResponse_Outcome{
			Message: retrySubscriptionWorkflowReply.GetOutcome().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			Message: retrySubscriptionWorkflowReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Parcel: &shippingv1.Parcel{
			Audit: &commonv1.AuditInfo{
				CreatedAt: checkCustomerSegmentEligibilityReply.GetCustomerSegment().GetAudit().GetCreatedAt(),
				UpdatedAt: recalculateCustomersReply.GetCustomer().GetAudit().GetUpdatedAt(),
			},
			Owner: &identityv1.UserRef{
				Email: pullManifestsReply.GetManifest().GetCounterparty().GetEmail(),
			},
			ParcelId: req.GetParcelId(),
			Capacity: req.GetParcel().GetCapacity(),
		},
	}, nil
}

// PauseParcelWorkflow pauses a parcel.
func (s *parcelWorkflowHandlers) PauseParcelWorkflow(ctx context.Context, req *shippingv1.PauseParcelWorkflowRequest) (*shippingv1.PauseParcelWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PauseParcelWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "pause parcel workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	var region string
	switch choice := req.GetParcel().GetLocator().(type) {
	case *shippingv1.Parcel_Address:
		region = choice.Address.GetRegion()
	case *shippingv1.Parcel_FacilityCode:
		region = choice.FacilityCode
	}
	specs := make(map[string]string, len(req.GetParcel().GetSpecs()))
	for key, value := range req.GetParcel().GetSpecs() {
		specs[key] = value
	}
	countTrackingEventTotalsRequest := &shippingv1.CountTrackingEventTotalsRequest{
		Context: req.GetContext(),
		Filter: &shippingv1.CountTrackingEventTotalsRequest_Filter{
			Region:       region,
			CreatedAfter: req.GetParcel().GetAudit().GetUpdatedAt(),
			Labels:       specs,
		},
	}
	countTrackingEventTotalsReply, err := grpcx.Call(ctx, s.trackingEventReportingClient.CountTrackingEventTotals, countTrackingEventTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventReportingService/CountTrackingEventTotals")
	}
	generateTrackingEventReportRequest := newGenerateTrackingEventReportRequest(withGenerateTrackingEventReportContext(req.GetContext()), withGenerateTrackingEventReportAnnotations(req.GetAnnotations()), withGenerateTrackingEventReportReason(req.GetReason()))
	generateTrackingEventReportReply, err := s.trackingEventReportingClient.GenerateTrackingEventReport(ctx, generateTrackingEventReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventReportingService/GenerateTrackingEventReport")
	}
	var region2 string
	switch choice2 := req.GetParcel().GetLocator().(type) {
	case *shippingv1.Parcel_Address:
		region2 = choice2.Address.GetRegion()
	case *shippingv1.Parcel_FacilityCode:
		region2 = choice2.FacilityCode
	}
	listPendingTiersRequest := &loyaltyv1.ListPendingTiersRequest{
		Context: req.GetContext(),
		Filter: &loyaltyv1.ListPendingTiersRequest_Filter{
			Region:       region2,
			CreatedAfter: req.GetParcel().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetParcel().GetComponents()["primary"].GetComponentId(),
		},
	}
	listPendingTiersReply, err := grpcx.NewClient(s.conn, loyaltyv1.NewTierApprovalServiceClient).ListPendingTiers(ctx, listPendingTiersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierApprovalService/ListPendingTiers")
	}
	_ = listPendingTiersReply
	return &shippingv1.PauseParcelWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status:       countTrackingEventTotalsReply.GetResult().GetStatus(),
			ReferenceIds: generateTrackingEventReportReply.GetResult().GetReferenceIds(),
		},
		Outcome: &shippingv1.PauseParcelWorkflowResponse_Outcome{
			Status: generateTrackingEventReportReply.GetResult().GetStatus(),
		},
		Parcel: &shippingv1.Parcel{
			ParcelId: req.GetParcelId(),
			Status:   req.GetParcel().GetStatus(),
		},
	}, nil
}

// ResumeParcelWorkflow resumes a parcel.
func (s *parcelWorkflowHandlers) ResumeParcelWorkflow(ctx context.Context, req *shippingv1.ResumeParcelWorkflowRequest) (*shippingv1.ResumeParcelWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "resume parcel workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	signalGiftCardWorkflowRequest := &giftcardsv1.SignalGiftCardWorkflowRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Contact: &commonv1.ContactInfo{Email: req.GetParcel().GetOwner().GetEmail()},
			Holder:  &giftcardsv1.GiftCardTransactionRef{DisplayName: req.GetParcel().GetOwner().GetDisplayName()},
		},
	}
	signalGiftCardWorkflowReply, err := grpcx.Call(ctx, s.giftCardWorkflowClient.SignalGiftCardWorkflow, signalGiftCardWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardWorkflowService/SignalGiftCardWorkflow")
	}
	encoded, err := protojson.Marshal(req.GetParcel())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded shippingv1.Parcel
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	approveShipmentRequest := &shippingv1.ApproveShipmentRequest{
		Context: req.GetContext(),
		Details: &shippingv1.ApproveShipmentRequest_Details{
			Approver: &identityv1.UserRef{
				Email: signalGiftCardWorkflowReply.GetGiftCard().GetContact().GetEmail(),
			},
		},
		Shipment: &shippingv1.Shipment{
			Audit: &commonv1.AuditInfo{UpdatedAt: (&decoded).GetAudit().GetUpdatedAt()},
			Settlement: &shippingv1.Shipment_Card{Card: &shippingv1.ShipmentCardSettlement{
				Last4: signalGiftCardWorkflowReply.GetGiftCard().GetCard().GetLast4(),
			}},
		},
	}
	approveShipmentReply, err := s.shipmentApprovalPort.ApproveShipment(ctx, approveShipmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentApprovalService/ApproveShipment")
	}
	return &shippingv1.ResumeParcelWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status: signalGiftCardWorkflowReply.GetOutcome().GetStatus(),
		},
		Outcome: &shippingv1.ResumeParcelWorkflowResponse_Outcome{NextSteps: approveShipmentReply.GetOutcome().GetNextSteps()},
		Parcel: &shippingv1.Parcel{
			ParcelId: req.GetParcelId(),
			Code:     req.GetParcel().GetCode(),
		},
	}, nil
}

// InspectParcelWorkflow returns a parcel.
func (s *parcelWorkflowHandlers) InspectParcelWorkflow(ctx context.Context, req *shippingv1.InspectParcelWorkflowRequest) (*shippingv1.InspectParcelWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected InspectParcelWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "parcel", req.GetParcelId(), "inspect_parcel_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect parcel workflow", "tenant", req.GetContext().GetTenantId())
	listSellerScorecardConflictsRequest := &sellersv1.ListSellerScorecardConflictsRequest{
		Context: req.GetContext(),
		Filter:  &sellersv1.ListSellerScorecardConflictsRequest_Filter{CreatedAfter: req.GetContext().GetDeadline()},
	}
	type listSellerScorecardConflictsResult struct {
		reply *sellersv1.ListSellerScorecardConflictsResponse
		err   error
	}
	listSellerScorecardConflictsResults := make(chan listSellerScorecardConflictsResult, 1)
	go func() {
		reply, err := s.sellerScorecardSyncClient.ListSellerScorecardConflicts(ctx, listSellerScorecardConflictsRequest)
		listSellerScorecardConflictsResults <- listSellerScorecardConflictsResult{reply, err}
	}()
	var listSellerScorecardConflictsReply *sellersv1.ListSellerScorecardConflictsResponse
	select {
	case result := <-listSellerScorecardConflictsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.sellers.v1.SellerScorecardSyncService/ListSellerScorecardConflicts")
		}
		listSellerScorecardConflictsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &shippingv1.InspectParcelWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: listSellerScorecardConflictsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Parcel: &shippingv1.Parcel{ParcelId: req.GetParcelId()},
	}, nil
}

// ListParcelWorkflows lists parcels a page at a time.
func (s *parcelWorkflowHandlers) ListParcelWorkflows(ctx context.Context, req *shippingv1.ListParcelWorkflowsRequest) (*shippingv1.ListParcelWorkflowsResponse, error) {
	s.log.InfoContext(ctx, "list parcel workflows", "tenant", req.GetContext().GetTenantId())
	return &shippingv1.ListParcelWorkflowsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SignalParcelWorkflow signals a parcel.
func (s *parcelWorkflowHandlers) SignalParcelWorkflow(ctx context.Context, req *shippingv1.SignalParcelWorkflowRequest) (*shippingv1.SignalParcelWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "signal parcel workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	lintNotificationsRequest := &notificationsv1.LintNotificationsRequest{}
	lintNotificationsRequest.Context = req.GetContext()
	lintNotificationsRequest.Notification = &notificationsv1.Notification{
		Requester: &commonv1.ContactInfo{
			PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
		},
	}
	lintNotificationsReply, err := s.notificationValidationClient.LintNotifications(ctx, lintNotificationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationValidationService/LintNotifications")
	}
	submitCarrierRateRequest := &shippingv1.SubmitCarrierRateRequest{
		Context: req.GetContext(),
		Details: &shippingv1.SubmitCarrierRateRequest_Details{
			Approver: &identityv1.UserRef{
				Email:       lintNotificationsReply.GetNotification().GetAssignee().GetEmail(),
				DisplayName: req.GetParcel().GetOwner().GetDisplayName(),
			},
		},
		CarrierRate: &shippingv1.CarrierRate{
			Audit: &commonv1.AuditInfo{
				CreatedBy: lintNotificationsReply.GetNotification().GetAudit().GetCreatedBy(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: lintNotificationsReply.GetNotification().GetCustomer().GetDisplayName(),
			},
		},
	}
	submitCarrierRateReply, err := s.carrierRateApprovalPort.SubmitCarrierRate(ctx, submitCarrierRateRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateApprovalService/SubmitCarrierRate")
	}
	_ = submitCarrierRateReply
	return &shippingv1.SignalParcelWorkflowResponse{
		Parcel: &shippingv1.Parcel{
			Owner: &identityv1.UserRef{
				DisplayName: lintNotificationsReply.GetNotification().GetCustomer().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: lintNotificationsReply.GetNotification().GetAudit().GetEtag(),
			},
			ParcelId: req.GetParcelId(),
			Name:     req.GetParcel().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
