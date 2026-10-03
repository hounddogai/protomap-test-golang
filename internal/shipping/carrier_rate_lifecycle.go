package shipping

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	contractsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/contracts/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// CarrierRateLifecycleServer implements `acme.shipping.v1.CarrierRateLifecycleService`.
// Moves a carrier rate between the states of its lifecycle.
type CarrierRateLifecycleServer struct {
	shippingv1.UnimplementedCarrierRateLifecycleServiceServer
	store                               *Store
	log                                 *slog.Logger
	conn                                *grpc.ClientConn
	preferenceValidationClient          customersv1.PreferenceValidationServiceClient
	bus                                 *eventbus.Bus
	customerClient                      customersv1.CustomerServiceClient
	shipmentCalculationClient           shippingv1.ShipmentCalculationServiceClient
	trackingEventValidationClient       shippingv1.TrackingEventValidationServiceClient
	parcelCalculationClient             shippingv1.ParcelCalculationServiceClient
	contractQueryClient                 contractsv1.ContractQueryServiceClient
	rewardApprovalClient                loyaltyv1.RewardApprovalServiceClient
	carrierRateReportingClient          shippingv1.CarrierRateReportingServiceClient
	sellerPayoutScheduleLifecycleClient sellersv1.SellerPayoutScheduleLifecycleServiceClient
	householdValidationClient           customersv1.HouseholdValidationServiceClient
}

// NewCarrierRateLifecycleServer wires a CarrierRateLifecycleServer.
func NewCarrierRateLifecycleServer(deps platform.Deps) *CarrierRateLifecycleServer {
	return &CarrierRateLifecycleServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.shipping.v1.CarrierRateLifecycleService"),
		conn:                                deps.Conn,
		preferenceValidationClient:          customersv1.NewPreferenceValidationServiceClient(deps.Conn),
		bus:                                 deps.Bus,
		customerClient:                      customersv1.NewCustomerServiceClient(deps.Conn),
		shipmentCalculationClient:           shippingv1.NewShipmentCalculationServiceClient(deps.Conn),
		trackingEventValidationClient:       shippingv1.NewTrackingEventValidationServiceClient(deps.Conn),
		parcelCalculationClient:             shippingv1.NewParcelCalculationServiceClient(deps.Conn),
		contractQueryClient:                 contractsv1.NewContractQueryServiceClient(deps.Conn),
		rewardApprovalClient:                loyaltyv1.NewRewardApprovalServiceClient(deps.Conn),
		carrierRateReportingClient:          shippingv1.NewCarrierRateReportingServiceClient(deps.Conn),
		sellerPayoutScheduleLifecycleClient: sellersv1.NewSellerPayoutScheduleLifecycleServiceClient(deps.Conn),
		householdValidationClient:           customersv1.NewHouseholdValidationServiceClient(deps.Conn),
	}
}

// ActivateCarrierRate activates a carrier rate.
func (s *CarrierRateLifecycleServer) ActivateCarrierRate(ctx context.Context, req *shippingv1.ActivateCarrierRateRequest) (*shippingv1.ActivateCarrierRateResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "carrier_rate", req.GetCarrierRateId(), "activate_carrier_rate"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate carrier rate", "tenant", req.GetContext().GetTenantId())
	listCustomersRequest := &customersv1.ListCustomersRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ListCustomersRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetCarrierRateId(),
		},
	}
	listCustomersReply := new(customersv1.ListCustomersResponse)
	if err := grpcx.Invoke(ctx, s.conn, customersv1.CustomerService_ListCustomers_FullMethodName, listCustomersRequest, listCustomersReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/ListCustomers")
	}
	previewPreferenceRequest := &customersv1.PreviewPreferenceRequest{}
	previewPreferenceRequest.Context = req.GetContext()
	previewPreferenceRequest.Details = &customersv1.PreviewPreferenceRequest_Details{
		Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNewOwner().GetEmail()},
	}
	previewPreferenceRequest.Preference = &customersv1.Preference{
		Owner: &identityv1.UserRef{UserId: req.GetDetails().GetNewOwner().GetUserId()},
	}
	previewPreferenceReply, err := s.preferenceValidationClient.PreviewPreference(ctx, previewPreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/PreviewPreference")
	}
	return &shippingv1.ActivateCarrierRateResponse{
		Outcome: &shippingv1.ActivateCarrierRateResponse_Outcome{
			Message:     listCustomersReply.GetResult().GetMessage(),
			Status:      listCustomersReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result:      &commonv1.OperationResult{Status: previewPreferenceReply.GetOutcome().GetStatus()},
		CarrierRate: &shippingv1.CarrierRate{CarrierRateId: req.GetCarrierRateId()},
	}, nil
}

// SuspendCarrierRate suspends a carrier rate.
func (s *CarrierRateLifecycleServer) SuspendCarrierRate(ctx context.Context, req *shippingv1.SuspendCarrierRateRequest) (*shippingv1.SuspendCarrierRateResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "carrier_rate", req.GetCarrierRateId(), "suspend_carrier_rate"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend carrier rate", "tenant", req.GetContext().GetTenantId())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "shipping.suspend_carrier_rate.preview_audit_event",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"details.contact.email": req.GetDetails().GetNotifyContact().GetEmail(),
			"context.tenant_id":     req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &shippingv1.SuspendCarrierRateResponse{
		CarrierRate: &shippingv1.CarrierRate{CarrierRateId: req.GetCarrierRateId()},
		Outcome:     &shippingv1.SuspendCarrierRateResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ReinstateCarrierRate reinstates a carrier rate.
func (s *CarrierRateLifecycleServer) ReinstateCarrierRate(ctx context.Context, req *shippingv1.ReinstateCarrierRateRequest) (*shippingv1.ReinstateCarrierRateResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "carrier_rate", req.GetCarrierRateId(), "reinstate_carrier_rate"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate carrier rate", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	listCustomersRequest := &customersv1.ListCustomersRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ListCustomersRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
			Labels:       annotations,
		},
	}
	var listCustomersReply *customersv1.ListCustomersResponse
	if err := pipeline.Run(ctx, "acme.customers.v1.CustomerService/ListCustomers", func(ctx context.Context) error {
		answer, err := s.customerClient.ListCustomers(ctx, listCustomersRequest)
		listCustomersReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/ListCustomers")
	}
	source := req.GetDetails().GetNewOwner().ProtoReflect()
	displayName := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("display_name"))).String()
	calculateShipmentRequest := &shippingv1.CalculateShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			Counterparty: &shippingv1.ParcelRef{DisplayName: displayName},
		},
		Annotations: req.GetAnnotations(),
	}
	calculateShipmentReply, err := grpcx.Call(ctx, s.shipmentCalculationClient.CalculateShipment, calculateShipmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/CalculateShipment")
	}
	return &shippingv1.ReinstateCarrierRateResponse{
		Outcome: &shippingv1.ReinstateCarrierRateResponse_Outcome{
			Status:      listCustomersReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: listCustomersReply.GetResult().GetStatus()},
		CarrierRate: &shippingv1.CarrierRate{
			Audit: &commonv1.AuditInfo{
				Etag: calculateShipmentReply.GetShipment().GetAudit().GetEtag(),
			},
			CarrierRateId: req.GetCarrierRateId(),
		},
	}, nil
}

// TerminateCarrierRate terminates a carrier rate.
func (s *CarrierRateLifecycleServer) TerminateCarrierRate(ctx context.Context, req *shippingv1.TerminateCarrierRateRequest) (*shippingv1.TerminateCarrierRateResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "carrier_rate", req.GetCarrierRateId(), "terminate_carrier_rate"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate carrier rate", "tenant", req.GetContext().GetTenantId())
	var holder struct{ Email string }
	if record := req.GetDetails().GetNewOwner(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	updateSearchIndexRequest := &searchv1.UpdateSearchIndexRequest{
		Context:     req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{Owner: &identityv1.UserRef{Email: holder.Email}},
	}
	updateSearchIndexReply := new(searchv1.UpdateSearchIndexResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", searchSearchIndexServiceName, "UpdateSearchIndex"), updateSearchIndexRequest, updateSearchIndexReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexService/UpdateSearchIndex")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainTrackingEventDecisionReply, err := s.trackingEventValidationClient.ExplainTrackingEventDecision(ctx, &shippingv1.ExplainTrackingEventDecisionRequest{
		Context: req.GetContext(),
		Details: &shippingv1.ExplainTrackingEventDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: updateSearchIndexReply.GetSearchIndex().GetOwner().GetEmail(),
				Phone: req.GetDetails().GetNotifyContact().GetPhone(),
			},
		},
		TrackingEvent: &shippingv1.TrackingEvent{
			CorrelationId: updateSearchIndexReply.GetResult().GetCorrelationId(),
			Detail:        packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventValidationService/ExplainTrackingEventDecision")
	}
	return &shippingv1.TerminateCarrierRateResponse{
		Result: &commonv1.OperationResult{
			Message:       updateSearchIndexReply.GetResult().GetMessage(),
			CorrelationId: explainTrackingEventDecisionReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		CarrierRate: &shippingv1.CarrierRate{
			Audit: &commonv1.AuditInfo{
				CreatedBy: updateSearchIndexReply.GetSearchIndex().GetAudit().GetCreatedBy(),
			},
			CarrierRateId: req.GetCarrierRateId(),
		},
		Outcome: &shippingv1.TerminateCarrierRateResponse_Outcome{
			Status:      explainTrackingEventDecisionReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	}, nil
}

// RenewCarrierRate renews a carrier rate.
func (s *CarrierRateLifecycleServer) RenewCarrierRate(ctx context.Context, req *shippingv1.RenewCarrierRateRequest) (*shippingv1.RenewCarrierRateResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "carrier_rate", req.GetCarrierRateId(), "renew_carrier_rate"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew carrier rate", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "user_id", req.GetDetails().GetNewOwner().GetUserId())
	estimateParcelReply, err := s.parcelCalculationClient.EstimateParcel(ctx, &shippingv1.EstimateParcelRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Owner: &identityv1.UserRef{UserId: tenancy.Value(ctx, "user_id")},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/EstimateParcel")
	}
	return &shippingv1.RenewCarrierRateResponse{
		Outcome: &shippingv1.RenewCarrierRateResponse_Outcome{
			Status:      estimateParcelReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		CarrierRate: &shippingv1.CarrierRate{CarrierRateId: req.GetCarrierRateId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExpireCarrierRate expires a carrier rate.
func (s *CarrierRateLifecycleServer) ExpireCarrierRate(ctx context.Context, req *shippingv1.ExpireCarrierRateRequest) (*shippingv1.ExpireCarrierRateResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "carrier_rate", req.GetCarrierRateId(), "expire_carrier_rate"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire carrier rate", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	suggestContractsRequest := &contractsv1.SuggestContractsRequest{
		Context: req.GetContext(),
		Filter: &contractsv1.SuggestContractsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
			Labels:       annotations,
		},
	}
	send := s.suggestContractsSender(suggestContractsRequest)
	suggestContractsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.contracts.v1.ContractQueryService/SuggestContracts")
	}
	rejectRewardRequest := newRejectRewardRequest(withRejectRewardContext(req.GetContext()), withRejectRewardDetails(&loyaltyv1.RejectRewardRequest_Details{
		Approver: &identityv1.UserRef{UserId: req.GetDetails().GetNewOwner().GetUserId()},
	}))
	rejectRewardReply, err := s.rewardApprovalClient.RejectReward(ctx, rejectRewardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardApprovalService/RejectReward")
	}
	return &shippingv1.ExpireCarrierRateResponse{
		Result: &commonv1.OperationResult{
			Details:      suggestContractsReply.GetResult().GetDetails(),
			ReferenceIds: suggestContractsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		CarrierRate: &shippingv1.CarrierRate{
			Owner:         &identityv1.UserRef{Email: rejectRewardReply.GetReward().GetOwner().GetEmail()},
			CarrierRateId: req.GetCarrierRateId(),
		},
		Outcome: &shippingv1.ExpireCarrierRateResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// GetCarrierRateLifecycle returns a carrier rate.
func (s *CarrierRateLifecycleServer) GetCarrierRateLifecycle(ctx context.Context, req *shippingv1.GetCarrierRateLifecycleRequest) (*shippingv1.GetCarrierRateLifecycleResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "carrier_rate", req.GetCarrierRateId(), "get_carrier_rate_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get carrier rate lifecycle", "tenant", req.GetContext().GetTenantId())
	return &shippingv1.GetCarrierRateLifecycleResponse{
		CarrierRate: &shippingv1.CarrierRate{CarrierRateId: req.GetCarrierRateId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScheduleCarrierRateExpiry schedules a carrier rate.
func (s *CarrierRateLifecycleServer) ScheduleCarrierRateExpiry(ctx context.Context, req *shippingv1.ScheduleCarrierRateExpiryRequest) (*shippingv1.ScheduleCarrierRateExpiryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCarrierRate(ctx, req.GetContext().GetTenantId(), req.GetCarrierRate()); err != nil {
		return nil, grpcx.Wrap(err, "save carrier rate")
	}
	s.log.InfoContext(ctx, "schedule carrier rate expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCarrierRate().GetAudit().GetEtag())
	streamCarrierRateMetricsStream, err := s.carrierRateReportingClient.StreamCarrierRateMetrics(ctx, &shippingv1.StreamCarrierRateMetricsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetDetails().GetNotify().GetEmail()},
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
	scheduleSellerPayoutScheduleExpiryRequest := buildScheduleSellerPayoutScheduleExpiryRequestForScheduleCarrierRateExpiry(req)
	scheduleSellerPayoutScheduleExpiryReply, err := s.sellerPayoutScheduleLifecycleClient.ScheduleSellerPayoutScheduleExpiry(ctx, scheduleSellerPayoutScheduleExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleLifecycleService/ScheduleSellerPayoutScheduleExpiry")
	}
	explainHouseholdDecisionRequest := &customersv1.ExplainHouseholdDecisionRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit: &commonv1.AuditInfo{
						UpdatedAt: req.GetCarrierRate().GetAudit().GetUpdatedAt(),
						CreatedAt: scheduleSellerPayoutScheduleExpiryReply.GetSellerPayoutSchedule().GetAudit().GetCreatedAt(),
						CreatedBy: scheduleSellerPayoutScheduleExpiryReply.GetSellerPayoutSchedule().GetAudit().GetCreatedBy(),
					},
				}},
			}},
		},
	}
	explainHouseholdDecisionReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*customersv1.ExplainHouseholdDecisionResponse, error) {
		return s.householdValidationClient.ExplainHouseholdDecision(ctx, explainHouseholdDecisionRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/ExplainHouseholdDecision")
	}
	return &shippingv1.ScheduleCarrierRateExpiryResponse{
		CarrierRate: &shippingv1.CarrierRate{
			Owner: &identityv1.UserRef{
				DisplayName: scheduleSellerPayoutScheduleExpiryReply.GetSellerPayoutSchedule().GetOrganizer().GetDisplayName(),
			},
			CarrierRateId: req.GetCarrierRateId(),
			Parameters:    req.GetCarrierRate().GetParameters(),
		},
		Result: &commonv1.OperationResult{
			Message: scheduleSellerPayoutScheduleExpiryReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &shippingv1.ScheduleCarrierRateExpiryResponse_Outcome{
			Status: explainHouseholdDecisionReply.GetResult().GetStatus(),
		},
	}, nil
}

// suggestContractsSender returns a function that sends one request to
// acme.contracts.v1.ContractQueryService/SuggestContracts.
func (s *CarrierRateLifecycleServer) suggestContractsSender(request *contractsv1.SuggestContractsRequest) func(context.Context) (*contractsv1.SuggestContractsResponse, error) {
	return func(ctx context.Context) (*contractsv1.SuggestContractsResponse, error) {
		return s.contractQueryClient.SuggestContracts(ctx, request)
	}
}
