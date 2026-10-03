package shipping

import (
	"context"
	"fmt"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	fuelv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fuel/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	performersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/performers/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// TrackingEventValidationServer implements `acme.shipping.v1.TrackingEventValidationService`.
// Checks tracking events against the rules before anything acts on them.
type TrackingEventValidationServer struct {
	shippingv1.UnimplementedTrackingEventValidationServiceServer
	store                           *Store
	log                             *slog.Logger
	shipmentCalculationClient       shippingv1.ShipmentCalculationServiceClient
	trackingEventCalculationClient  shippingv1.TrackingEventCalculationServiceClient
	renewalScheduleValidationClient subscriptionsv1.RenewalScheduleValidationServiceClient
	meteredShipmentValidation       meteredShipmentValidation
	carrierRateCalculationClient    shippingv1.CarrierRateCalculationServiceClient
	parcelValidationClient          shippingv1.ParcelValidationServiceClient
	conn                            *grpc.ClientConn
	searchQueryQueryClient          searchv1.SearchQueryQueryServiceClient
	searchQueryValidationClient     searchv1.SearchQueryValidationServiceClient
	reviewVoteValidationClient      reviewsv1.ReviewVoteValidationServiceClient
	clients                         *registry.Registry
	meteredPerformerQuery           meteredPerformerQuery
}

// NewTrackingEventValidationServer wires a TrackingEventValidationServer.
func NewTrackingEventValidationServer(deps platform.Deps) *TrackingEventValidationServer {
	return &TrackingEventValidationServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.shipping.v1.TrackingEventValidationService"),
		shipmentCalculationClient:       shippingv1.NewShipmentCalculationServiceClient(deps.Conn),
		trackingEventCalculationClient:  shippingv1.NewTrackingEventCalculationServiceClient(deps.Conn),
		renewalScheduleValidationClient: subscriptionsv1.NewRenewalScheduleValidationServiceClient(deps.Conn),
		meteredShipmentValidation:       meteredShipmentValidation{ShipmentValidationServiceClient: shippingv1.NewShipmentValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		carrierRateCalculationClient:    shippingv1.NewCarrierRateCalculationServiceClient(deps.Conn),
		parcelValidationClient:          shippingv1.NewParcelValidationServiceClient(deps.Conn),
		conn:                            deps.Conn,
		searchQueryQueryClient:          searchv1.NewSearchQueryQueryServiceClient(deps.Conn),
		searchQueryValidationClient:     searchv1.NewSearchQueryValidationServiceClient(deps.Conn),
		reviewVoteValidationClient:      reviewsv1.NewReviewVoteValidationServiceClient(deps.Conn),
		clients:                         deps.Clients,
		meteredPerformerQuery:           meteredPerformerQuery{PerformerQueryServiceClient: performersv1.NewPerformerQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}
}

// ValidateTrackingEvent validates a tracking event.
func (s *TrackingEventValidationServer) ValidateTrackingEvent(ctx context.Context, req *shippingv1.ValidateTrackingEventRequest) (*shippingv1.ValidateTrackingEventResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveTrackingEvent(ctx, req.GetContext().GetTenantId(), req.GetTrackingEvent()); err != nil {
		return nil, grpcx.Wrap(err, "save tracking event")
	}
	s.log.InfoContext(ctx, "validate tracking event", "tenant", req.GetContext().GetTenantId())
	var envelope shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	estimateShipmentRequest := &shippingv1.EstimateShipmentRequest{}
	estimateShipmentRequest.Context = req.GetContext()
	estimateShipmentRequest.ShipmentId = req.GetTrackingEvent().GetSubject().GetShipmentId()
	estimateShipmentRequest.Shipment = &shippingv1.Shipment{ShipmentId: envelope.GetDisplayName()}
	estimateShipmentReply, err := s.shipmentCalculationClient.EstimateShipment(ctx, estimateShipmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/EstimateShipment")
	}
	getTrackingEventRatesReply, err := s.trackingEventCalculationClient.GetTrackingEventRates(ctx, &shippingv1.GetTrackingEventRatesRequest{
		Context:         req.GetContext(),
		TrackingEventId: req.GetTrackingEvent().GetTrackingEventId(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventCalculationService/GetTrackingEventRates")
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded shippingv1.ValidateTrackingEventRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	var envelope2 shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	step := &trackingEventValidationServerValidateTrackingEventCheckRenewalScheduleEligibilityStep{request: &subscriptionsv1.CheckRenewalScheduleEligibilityRequest{
		Context: req.GetContext(),
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Participants: []*subscriptionsv1.RenewalScheduleParticipant{&subscriptionsv1.RenewalScheduleParticipant{
				Contact: &commonv1.ContactInfo{Phone: (&decoded).GetContact().GetPhone()},
			}},
			Location: &commonv1.PostalAddress{
				Lines: estimateShipmentReply.GetShipment().GetBillingAddress().GetLines(),
			},
		},
		RenewalScheduleId: envelope2.GetDisplayName(),
	}}
	if err := s.runValidateTrackingEventCheckRenewalScheduleEligibility(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.RenewalScheduleValidationService/CheckRenewalScheduleEligibility")
	}
	checkRenewalScheduleEligibilityReply := step.reply
	return &shippingv1.ValidateTrackingEventResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: estimateShipmentReply.GetResult().GetReferenceIds(),
			Status:       estimateShipmentReply.GetResult().GetStatus(),
		},
		TrackingEvent: &shippingv1.TrackingEvent{
			Source:          getTrackingEventRatesReply.GetTrackingEvent().GetSource(),
			CorrelationId:   checkRenewalScheduleEligibilityReply.GetResult().GetCorrelationId(),
			TrackingEventId: req.GetTrackingEventId(),
			Subject:         req.GetTrackingEvent().GetSubject(),
		},
	}, nil
}

// PreviewTrackingEvent previews a tracking event.
func (s *TrackingEventValidationServer) PreviewTrackingEvent(ctx context.Context, req *shippingv1.PreviewTrackingEventRequest) (*shippingv1.PreviewTrackingEventResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveTrackingEvent(ctx, req.GetContext().GetTenantId(), req.GetTrackingEvent()); err != nil {
		return nil, grpcx.Wrap(err, "save tracking event")
	}
	s.log.InfoContext(ctx, "preview tracking event", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "preferred_channel", req.GetDetails().GetContact().GetPreferredChannel())
	attributes := make(map[string]string, len(req.GetTrackingEvent().GetAttributes()))
	for key, value := range req.GetTrackingEvent().GetAttributes() {
		attributes[key] = value
	}
	encoded, err := protojson.Marshal(req.GetTrackingEvent())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded shippingv1.TrackingEvent
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	var envelope shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	normalizeShipmentRequest := &shippingv1.NormalizeShipmentRequest{
		Context: req.GetContext(),
		Details: &shippingv1.NormalizeShipmentRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone:            req.GetDetails().GetContact().GetPhone(),
				PreferredChannel: tenancy.Value(ctx, "preferred_channel"),
			},
		},
		Shipment: &shippingv1.Shipment{
			Lines:  []*shippingv1.ShipmentLine{&shippingv1.ShipmentLine{Attributes: attributes}},
			Number: (&decoded).GetSubject().GetNumber(),
		},
		ShipmentId: envelope.GetDisplayName(),
	}
	normalizeShipmentReply, err := s.meteredShipmentValidation.NormalizeShipment(ctx, normalizeShipmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentValidationService/NormalizeShipment")
	}
	return &shippingv1.PreviewTrackingEventResponse{
		TrackingEvent: &shippingv1.TrackingEvent{
			Subject: &shippingv1.ShipmentRef{
				DisplayName: normalizeShipmentReply.GetShipment().GetCounterparty().GetDisplayName(),
			},
			TrackingEventId: req.GetTrackingEventId(),
			Sequence:        req.GetTrackingEvent().GetSequence(),
		},
		Outcome: &shippingv1.PreviewTrackingEventResponse_Outcome{Status: normalizeShipmentReply.GetOutcome().GetStatus()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckTrackingEventEligibility checks a tracking event.
func (s *TrackingEventValidationServer) CheckTrackingEventEligibility(ctx context.Context, req *shippingv1.CheckTrackingEventEligibilityRequest) (*shippingv1.CheckTrackingEventEligibilityResponse, error) {
	if err := s.store.SaveTrackingEvent(ctx, req.GetContext().GetTenantId(), req.GetTrackingEvent()); err != nil {
		return nil, grpcx.Wrap(err, "save tracking event")
	}
	s.log.InfoContext(ctx, "check tracking event eligibility", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetTrackingEvent())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded shippingv1.TrackingEvent
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	var envelope shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	explainCarrierRateCalculationReply, err := s.carrierRateCalculationClient.ExplainCarrierRateCalculation(ctx, &shippingv1.ExplainCarrierRateCalculationRequest{
		Context: req.GetContext(),
		CarrierRate: &shippingv1.CarrierRate{
			Owner:  &identityv1.UserRef{DisplayName: (&decoded).GetSubject().GetDisplayName()},
			Action: &shippingv1.CarrierRate_Block{Block: &shippingv1.CarrierRateBlockAction{NotifyEmail: req.GetDetails().GetContact().GetEmail()}},
		},
		Details:       &shippingv1.ExplainCarrierRateCalculationRequest_Details{Rules: req.GetDetails().GetRules()},
		CarrierRateId: envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateCalculationService/ExplainCarrierRateCalculation")
	}
	return &shippingv1.CheckTrackingEventEligibilityResponse{
		Result: &commonv1.OperationResult{
			Status:        explainCarrierRateCalculationReply.GetOutcome().GetStatus(),
			CorrelationId: req.GetTrackingEvent().GetCorrelationId(),
		},
		TrackingEvent: &shippingv1.TrackingEvent{TrackingEventId: req.GetTrackingEventId()},
	}, nil
}

// NormalizeTrackingEvent normalizes a tracking event.
func (s *TrackingEventValidationServer) NormalizeTrackingEvent(ctx context.Context, req *shippingv1.NormalizeTrackingEventRequest) (*shippingv1.NormalizeTrackingEventResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected NormalizeTrackingEvent", "error", err)
		return nil, err
	}
	if err := s.store.SaveTrackingEvent(ctx, req.GetContext().GetTenantId(), req.GetTrackingEvent()); err != nil {
		return nil, grpcx.Wrap(err, "save tracking event")
	}
	s.log.InfoContext(ctx, "normalize tracking event", "tenant", req.GetContext().GetTenantId())
	lintParcelsRequest := &shippingv1.LintParcelsRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetTrackingEvent().GetSubject().GetDisplayName(),
			},
		},
		Details: &shippingv1.LintParcelsRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
				Name:             req.GetDetails().GetContact().GetName(),
			},
		},
	}
	send := s.lintParcelsSender(lintParcelsRequest)
	lintParcelsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelValidationService/LintParcels")
	}
	var envelope shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	explainShipmentCalculationReply, err := s.shipmentCalculationClient.ExplainShipmentCalculation(ctx, &shippingv1.ExplainShipmentCalculationRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{Lines: lintParcelsReply.GetParcel().GetAddress().GetLines()},
		},
		ShipmentId: envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/ExplainShipmentCalculation")
	}
	var envelope2 shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	validateParcelRequest := &shippingv1.ValidateParcelRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Owner: &identityv1.UserRef{UserId: lintParcelsReply.GetParcel().GetOwner().GetUserId()},
			Dimensions: &shippingv1.ParcelDimensions{
				Width: lintParcelsReply.GetParcel().GetDimensions().GetWidth(),
			},
			Locator: &shippingv1.Parcel_Address{Address: &commonv1.PostalAddress{
				PostalCode: explainShipmentCalculationReply.GetShipment().GetBillingAddress().GetPostalCode(),
			}},
		},
		Details: &shippingv1.ValidateParcelRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetContact().GetName()},
		},
		ParcelId: envelope2.GetDisplayName(),
	}
	validateParcelReply := new(shippingv1.ValidateParcelResponse)
	if err := grpcx.Invoke(ctx, s.conn, shippingv1.ParcelValidationService_ValidateParcel_FullMethodName, validateParcelRequest, validateParcelReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelValidationService/ValidateParcel")
	}
	var envelope3 shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope3); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	estimateFuelCardRequest := &fuelv1.EstimateFuelCardRequest{
		Context: req.GetContext(),
		Details: &fuelv1.EstimateFuelCardRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region: explainShipmentCalculationReply.GetShipment().GetBillingAddress().GetRegion(),
			},
		},
		FuelCardId: envelope3.GetDisplayName(),
	}
	estimateFuelCardReply, err := estimateFuelCardCall(ctx, estimateFuelCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fuel.v1.FuelCardCalculationService/EstimateFuelCard")
	}
	_ = estimateFuelCardReply
	return &shippingv1.NormalizeTrackingEventResponse{
		Outcome: &shippingv1.NormalizeTrackingEventResponse_Outcome{
			Status:  lintParcelsReply.GetOutcome().GetStatus(),
			Message: lintParcelsReply.GetResult().GetMessage(),
		},
		TrackingEvent: &shippingv1.TrackingEvent{
			Subject: &shippingv1.ShipmentRef{
				Number: explainShipmentCalculationReply.GetShipment().GetNumber(),
			},
			TrackingEventId: req.GetTrackingEventId(),
			Source:          req.GetTrackingEvent().GetSource(),
		},
		Result: &commonv1.OperationResult{
			Message: validateParcelReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// ExplainTrackingEventDecision explains a tracking event.
func (s *TrackingEventValidationServer) ExplainTrackingEventDecision(ctx context.Context, req *shippingv1.ExplainTrackingEventDecisionRequest) (*shippingv1.ExplainTrackingEventDecisionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveTrackingEvent(ctx, req.GetContext().GetTenantId(), req.GetTrackingEvent()); err != nil {
		return nil, grpcx.Wrap(err, "save tracking event")
	}
	s.log.InfoContext(ctx, "explain tracking event decision", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "tracking_event_id", req.GetTrackingEvent().GetTrackingEventId())
	var envelope shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	findSearchQueriesRequest := &searchv1.FindSearchQueriesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.FindSearchQueriesRequest_Filter{
			CreatedAfter: req.GetTrackingEvent().GetOccurredAt(),
			Labels:       req.GetTrackingEvent().GetAttributes(),
			OwnerId:      tenancy.Value(ctx, "tracking_event_id"),
		},
		Page: &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	findSearchQueriesReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*searchv1.FindSearchQueriesResponse, error) {
		return s.searchQueryQueryClient.FindSearchQueries(ctx, findSearchQueriesRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/FindSearchQueries")
	}
	for _, measurement := range req.GetTrackingEvent().GetMeasurements() {
		if _, err := s.searchQueryValidationClient.ScoreSearchQuery(ctx, &searchv1.ScoreSearchQueryRequest{
			Context: req.GetContext(),
			SearchQuery: &searchv1.SearchQuery{
				Measurements: []*searchv1.SearchQueryMeasurement{&searchv1.SearchQueryMeasurement{ObservedAt: measurement.GetObservedAt()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryValidationService/ScoreSearchQuery")
		}
	}
	ctx = tenancy.With(ctx, "phone", req.GetDetails().GetContact().GetPhone())
	packed, err := anypb.New(req.GetTrackingEvent())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope2 shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	explainReviewVoteDecisionReply, err := s.reviewVoteValidationClient.ExplainReviewVoteDecision(ctx, &reviewsv1.ExplainReviewVoteDecisionRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.ExplainReviewVoteDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone: tenancy.Value(ctx, "phone"),
				Email: req.GetDetails().GetContact().GetEmail(),
			},
		},
		ReviewVote:   &reviewsv1.ReviewVote{Detail: packed},
		ReviewVoteId: envelope2.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteValidationService/ExplainReviewVoteDecision")
	}
	ctx = tenancy.With(ctx, "phone", req.GetDetails().GetContact().GetPhone())
	normalizeCarrierRateRequest := &shippingv1.NormalizeCarrierRateRequest{
		Context: req.GetContext(),
		CarrierRate: &shippingv1.CarrierRate{
			Owner: &identityv1.UserRef{
				Email:       explainReviewVoteDecisionReply.GetReviewVote().GetSubject().GetEmail(),
				DisplayName: explainReviewVoteDecisionReply.GetReviewVote().GetSubject().GetDisplayName(),
			},
		},
		Details: &shippingv1.NormalizeCarrierRateRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:  req.GetDetails().GetContact().GetName(),
				Phone: tenancy.Value(ctx, "phone"),
			},
		},
	}
	normalizeCarrierRateReply := new(shippingv1.NormalizeCarrierRateResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", shippingCarrierRateValidationServiceName, "NormalizeCarrierRate"), normalizeCarrierRateRequest, normalizeCarrierRateReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateValidationService/NormalizeCarrierRate")
	}
	return &shippingv1.ExplainTrackingEventDecisionResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: findSearchQueriesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &shippingv1.ExplainTrackingEventDecisionResponse_Outcome{
			Status:      explainReviewVoteDecisionReply.GetResult().GetStatus(),
			ReferenceId: normalizeCarrierRateReply.GetOutcome().GetReferenceId(),
		},
		TrackingEvent: &shippingv1.TrackingEvent{
			TrackingEventId: req.GetTrackingEventId(),
			CorrelationId:   req.GetTrackingEvent().GetCorrelationId(),
		},
	}, nil
}

// ScoreTrackingEvent scores a tracking event.
func (s *TrackingEventValidationServer) ScoreTrackingEvent(ctx context.Context, req *shippingv1.ScoreTrackingEventRequest) (*shippingv1.ScoreTrackingEventResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveTrackingEvent(ctx, req.GetContext().GetTenantId(), req.GetTrackingEvent()); err != nil {
		return nil, grpcx.Wrap(err, "save tracking event")
	}
	s.log.InfoContext(ctx, "score tracking event", "tenant", req.GetContext().GetTenantId())
	trackingEventCalculation, err := registry.Lookup[shippingv1.TrackingEventCalculationServiceClient](s.clients, "acme.shipping.v1.TrackingEventCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	getTrackingEventRatesRequest := &shippingv1.GetTrackingEventRatesRequest{
		Context:         req.GetContext(),
		TrackingEventId: req.GetTrackingEventId(),
	}
	getTrackingEventRatesReply, err := trackingEventCalculation.GetTrackingEventRates(ctx, getTrackingEventRatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventCalculationService/GetTrackingEventRates")
	}
	var envelope shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	previewShipmentRequest := &shippingv1.PreviewShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{
				Lines:       req.GetDetails().GetDestination().GetLines(),
				PostalCode:  req.GetDetails().GetDestination().GetPostalCode(),
				CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
			},
			Counterparty: &shippingv1.ParcelRef{
				DisplayName: getTrackingEventRatesReply.GetTrackingEvent().GetSubject().GetDisplayName(),
			},
		},
		ShipmentId: envelope.GetDisplayName(),
	}
	previewShipmentReply := new(shippingv1.PreviewShipmentResponse)
	if err := s.conn.Invoke(ctx, shippingShipmentValidationServicePreviewShipmentMethod, previewShipmentRequest, previewShipmentReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentValidationService/PreviewShipment")
	}
	return &shippingv1.ScoreTrackingEventResponse{
		TrackingEvent: &shippingv1.TrackingEvent{
			CorrelationId:   getTrackingEventRatesReply.GetResult().GetCorrelationId(),
			Subject:         &shippingv1.ShipmentRef{Number: previewShipmentReply.GetShipment().GetNumber()},
			TrackingEventId: req.GetTrackingEventId(),
			Kind:            req.GetTrackingEvent().GetKind(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LintTrackingEvents lints a tracking event.
func (s *TrackingEventValidationServer) LintTrackingEvents(ctx context.Context, req *shippingv1.LintTrackingEventsRequest) (*shippingv1.LintTrackingEventsResponse, error) {
	if err := s.store.SaveTrackingEvent(ctx, req.GetContext().GetTenantId(), req.GetTrackingEvent()); err != nil {
		return nil, grpcx.Wrap(err, "save tracking event")
	}
	s.log.InfoContext(ctx, "lint tracking events", "tenant", req.GetContext().GetTenantId())
	var envelope shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	checkParcelEligibilityRequest := &shippingv1.CheckParcelEligibilityRequest{
		Context: req.GetContext(),
		Details: &shippingv1.CheckParcelEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
		},
		ParcelId: envelope.GetDisplayName(),
	}
	checkParcelEligibilityReply, err := grpcx.Call(ctx, s.parcelValidationClient.CheckParcelEligibility, checkParcelEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelValidationService/CheckParcelEligibility")
	}
	var envelope2 shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	countPerformersRequest := &performersv1.CountPerformersRequest{
		Context: req.GetContext(),
		Filter: &performersv1.CountPerformersRequest_Filter{
			Region:       checkParcelEligibilityReply.GetParcel().GetAddress().GetRegion(),
			CreatedAfter: req.GetTrackingEvent().GetOccurredAt(),
			OwnerId:      req.GetTrackingEvent().GetSubject().GetShipmentId(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope2.GetDisplayName()},
	}
	countPerformersReply, err := s.meteredPerformerQuery.CountPerformers(ctx, countPerformersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.performers.v1.PerformerQueryService/CountPerformers")
	}
	return &shippingv1.LintTrackingEventsResponse{
		Outcome: &shippingv1.LintTrackingEventsResponse_Outcome{
			NextSteps: checkParcelEligibilityReply.GetOutcome().GetNextSteps(),
			Status:    countPerformersReply.GetResult().GetStatus(),
		},
		TrackingEvent: &shippingv1.TrackingEvent{
			TrackingEventId: req.GetTrackingEventId(),
			Attributes:      req.GetTrackingEvent().GetAttributes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetTrackingEventRules returns a tracking event.
func (s *TrackingEventValidationServer) GetTrackingEventRules(ctx context.Context, req *shippingv1.GetTrackingEventRulesRequest) (*shippingv1.GetTrackingEventRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tracking_event", req.GetTrackingEventId(), "get_tracking_event_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get tracking event rules", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	estimateTrackingEventRequest := &shippingv1.EstimateTrackingEventRequest{
		Context: req.GetContext(),
		TrackingEvent: &shippingv1.TrackingEvent{
			TrackingEventId: req.GetTrackingEventId(),
			Measurements:    []*shippingv1.TrackingEventMeasurement{&shippingv1.TrackingEventMeasurement{ObservedAt: req.GetContext().GetDeadline()}},
			Source:          req.GetContext().GetActor().GetUserId(),
			Detail:          packed,
		},
	}
	estimateTrackingEventReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*shippingv1.EstimateTrackingEventResponse, error) {
		return s.trackingEventCalculationClient.EstimateTrackingEvent(ctx, estimateTrackingEventRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventCalculationService/EstimateTrackingEvent")
	}
	return &shippingv1.GetTrackingEventRulesResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: estimateTrackingEventReply.GetTrackingEvent().GetCorrelationId(),
			Status:        "ok",
		},
		TrackingEvent: &shippingv1.TrackingEvent{
			Subject:         estimateTrackingEventReply.GetTrackingEvent().GetSubject(),
			TrackingEventId: req.GetTrackingEventId(),
		},
	}, nil
}

// trackingEventValidationServerValidateTrackingEventCheckRenewalScheduleEligibilityStep carries the request of one step
// of ValidateTrackingEvent and, once it ran, its reply.
type trackingEventValidationServerValidateTrackingEventCheckRenewalScheduleEligibilityStep struct {
	request *subscriptionsv1.CheckRenewalScheduleEligibilityRequest
	reply   *subscriptionsv1.CheckRenewalScheduleEligibilityResponse
}

func (s *TrackingEventValidationServer) runValidateTrackingEventCheckRenewalScheduleEligibility(ctx context.Context, step *trackingEventValidationServerValidateTrackingEventCheckRenewalScheduleEligibilityStep) error {
	reply, err := s.renewalScheduleValidationClient.CheckRenewalScheduleEligibility(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// lintParcelsSender returns a function that sends one request to acme.shipping.v1.ParcelValidationService/LintParcels.
func (s *TrackingEventValidationServer) lintParcelsSender(request *shippingv1.LintParcelsRequest) func(context.Context) (*shippingv1.LintParcelsResponse, error) {
	return func(ctx context.Context) (*shippingv1.LintParcelsResponse, error) {
		return s.parcelValidationClient.LintParcels(ctx, request)
	}
}
