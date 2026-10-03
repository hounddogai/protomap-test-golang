package shipping

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	recruitingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/recruiting/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// TrackingEventCalculationServer implements `acme.shipping.v1.TrackingEventCalculationService`.
// Computes the figures of a tracking event, such as totals, taxes, and estimates.
type TrackingEventCalculationServer struct {
	shippingv1.UnimplementedTrackingEventCalculationServiceServer
	store                         *Store
	log                           *slog.Logger
	conn                          *grpc.ClientConn
	shipmentValidationPort        shipmentValidationPort
	accessPolicyValidationClient  authorizationv1.AccessPolicyValidationServiceClient
	meteredShipmentValidation     meteredShipmentValidation
	sellerCalculationClient       sellersv1.SellerCalculationServiceClient
	shipmentValidationClient      shippingv1.ShipmentValidationServiceClient
	jobOpeningQueryClient         recruitingv1.JobOpeningQueryServiceClient
	searchIndexNotificationClient searchv1.SearchIndexNotificationServiceClient
	bus                           *eventbus.Bus
	parcelCalculationClient       shippingv1.ParcelCalculationServiceClient
}

// NewTrackingEventCalculationServer wires a TrackingEventCalculationServer.
func NewTrackingEventCalculationServer(deps platform.Deps) *TrackingEventCalculationServer {
	return &TrackingEventCalculationServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.shipping.v1.TrackingEventCalculationService"),
		conn:                          deps.Conn,
		shipmentValidationPort:        newShipmentValidationPort(deps),
		accessPolicyValidationClient:  authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn),
		meteredShipmentValidation:     meteredShipmentValidation{ShipmentValidationServiceClient: shippingv1.NewShipmentValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		sellerCalculationClient:       sellersv1.NewSellerCalculationServiceClient(deps.Conn),
		shipmentValidationClient:      shippingv1.NewShipmentValidationServiceClient(deps.Conn),
		jobOpeningQueryClient:         recruitingv1.NewJobOpeningQueryServiceClient(deps.Conn),
		searchIndexNotificationClient: searchv1.NewSearchIndexNotificationServiceClient(deps.Conn),
		bus:                           deps.Bus,
		parcelCalculationClient:       shippingv1.NewParcelCalculationServiceClient(deps.Conn),
	}
}

// CalculateTrackingEvent calculates a tracking event.
func (s *TrackingEventCalculationServer) CalculateTrackingEvent(ctx context.Context, req *shippingv1.CalculateTrackingEventRequest) (*shippingv1.CalculateTrackingEventResponse, error) {
	if err := s.store.SaveTrackingEvent(ctx, req.GetContext().GetTenantId(), req.GetTrackingEvent()); err != nil {
		return nil, grpcx.Wrap(err, "save tracking event")
	}
	s.log.InfoContext(ctx, "calculate tracking event", "tenant", req.GetContext().GetTenantId())
	explainParcelCalculationRequest := &shippingv1.ExplainParcelCalculationRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Locator: &shippingv1.Parcel_Address{Address: &commonv1.PostalAddress{
				Locality: req.GetDetails().GetDestination().GetLocality(),
				Region:   req.GetDetails().GetDestination().GetRegion(),
			}},
		},
	}
	explainParcelCalculationReply := new(shippingv1.ExplainParcelCalculationResponse)
	if err := grpcx.Invoke(ctx, s.conn, shippingv1.ParcelCalculationService_ExplainParcelCalculation_FullMethodName, explainParcelCalculationRequest, explainParcelCalculationReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/ExplainParcelCalculation")
	}
	var envelope shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	lintMerchandisingRulesRequest := &merchandisingv1.LintMerchandisingRulesRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Audit: &commonv1.AuditInfo{
				CreatedBy: explainParcelCalculationReply.GetParcel().GetAudit().GetCreatedBy(),
			},
		},
		MerchandisingRuleId: envelope.GetDisplayName(),
	}
	lintMerchandisingRulesReply := new(merchandisingv1.LintMerchandisingRulesResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", merchandisingMerchandisingRuleValidationServiceName, "LintMerchandisingRules"), lintMerchandisingRulesRequest, lintMerchandisingRulesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleValidationService/LintMerchandisingRules")
	}
	getShipmentRulesRequest := &shippingv1.GetShipmentRulesRequest{
		Context:    req.GetContext(),
		ShipmentId: req.GetTrackingEvent().GetSubject().GetShipmentId(),
	}
	getShipmentRulesReply, err := s.shipmentValidationPort.GetShipmentRules(ctx, getShipmentRulesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentValidationService/GetShipmentRules")
	}
	var envelope2 shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	scoreAccessPolicyRequest := &authorizationv1.ScoreAccessPolicyRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Action: &authorizationv1.AccessPolicy_Discount{Discount: &authorizationv1.AccessPolicyDiscountAction{
				Percent: lintMerchandisingRulesReply.GetMerchandisingRule().GetDiscount().GetPercent(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: lintMerchandisingRulesReply.GetMerchandisingRule().GetAudit().GetCreatedAt(),
			},
		},
		Details: &authorizationv1.ScoreAccessPolicyRequest_Details{
			Destination: &commonv1.PostalAddress{Recipient: req.GetDetails().GetDestination().GetRecipient()},
		},
		AccessPolicyId: envelope2.GetDisplayName(),
	}
	type scoreAccessPolicyResult struct {
		reply *authorizationv1.ScoreAccessPolicyResponse
		err   error
	}
	scoreAccessPolicyResults := make(chan scoreAccessPolicyResult, 1)
	go func() {
		reply, err := s.accessPolicyValidationClient.ScoreAccessPolicy(ctx, scoreAccessPolicyRequest)
		scoreAccessPolicyResults <- scoreAccessPolicyResult{reply, err}
	}()
	var scoreAccessPolicyReply *authorizationv1.ScoreAccessPolicyResponse
	select {
	case result := <-scoreAccessPolicyResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.authorization.v1.AccessPolicyValidationService/ScoreAccessPolicy")
		}
		scoreAccessPolicyReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &shippingv1.CalculateTrackingEventResponse{
		Result: &commonv1.OperationResult{
			Status:        explainParcelCalculationReply.GetOutcome().GetStatus(),
			Message:       lintMerchandisingRulesReply.GetResult().GetMessage(),
			ReferenceIds:  lintMerchandisingRulesReply.GetResult().GetReferenceIds(),
			CorrelationId: getShipmentRulesReply.GetResult().GetCorrelationId(),
		},
		TrackingEvent: &shippingv1.TrackingEvent{
			Subject: &shippingv1.ShipmentRef{
				DisplayName: scoreAccessPolicyReply.GetAccessPolicy().GetOwner().GetDisplayName(),
			},
			TrackingEventId: req.GetTrackingEventId(),
		},
		Breakdown: &shippingv1.CalculateTrackingEventResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
	}, nil
}

// EstimateTrackingEvent estimates a tracking event.
func (s *TrackingEventCalculationServer) EstimateTrackingEvent(ctx context.Context, req *shippingv1.EstimateTrackingEventRequest) (*shippingv1.EstimateTrackingEventResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveTrackingEvent(ctx, req.GetContext().GetTenantId(), req.GetTrackingEvent()); err != nil {
		return nil, grpcx.Wrap(err, "save tracking event")
	}
	s.log.InfoContext(ctx, "estimate tracking event", "tenant", req.GetContext().GetTenantId())
	var envelope shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	normalizeShipmentRequest := &shippingv1.NormalizeShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{Lines: req.GetDetails().GetDestination().GetLines()},
		},
		ShipmentId: envelope.GetDisplayName(),
	}
	normalizeShipmentReply, err := s.meteredShipmentValidation.NormalizeShipment(ctx, normalizeShipmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentValidationService/NormalizeShipment")
	}
	cloned := proto.Clone(req.GetTrackingEvent()).(*shippingv1.TrackingEvent)
	step := &trackingEventCalculationServerEstimateTrackingEventExplainSellerCalculationStep{request: &sellersv1.ExplainSellerCalculationRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{DisplayName: cloned.GetSubject().GetDisplayName()},
			PrimaryAddress: &commonv1.PostalAddress{
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: normalizeShipmentReply.GetShipment().GetAudit().GetCreatedAt(),
			},
		},
	}}
	if err := s.runEstimateTrackingEventExplainSellerCalculation(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/ExplainSellerCalculation")
	}
	explainSellerCalculationReply := step.reply
	ctx = tenancy.With(ctx, "shipment_id", req.GetTrackingEvent().GetSubject().GetShipmentId())
	getShipmentRulesRequest := &shippingv1.GetShipmentRulesRequest{}
	getShipmentRulesRequest.Context = req.GetContext()
	getShipmentRulesRequest.ShipmentId = tenancy.Value(ctx, "shipment_id")
	getShipmentRulesReply, err := s.shipmentValidationClient.GetShipmentRules(ctx, getShipmentRulesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentValidationService/GetShipmentRules")
	}
	return &shippingv1.EstimateTrackingEventResponse{
		Result: &commonv1.OperationResult{
			Details:       normalizeShipmentReply.GetResult().GetDetails(),
			Status:        explainSellerCalculationReply.GetOutcome().GetStatus(),
			CorrelationId: getShipmentRulesReply.GetResult().GetCorrelationId(),
		},
		TrackingEvent: &shippingv1.TrackingEvent{
			Subject: &shippingv1.ShipmentRef{
				Number: explainSellerCalculationReply.GetSeller().GetNationalId().GetNumber(),
			},
			TrackingEventId: req.GetTrackingEventId(),
		},
		Breakdown: &shippingv1.EstimateTrackingEventResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
	}, nil
}

// CompareTrackingEventOptions compares a tracking event.
func (s *TrackingEventCalculationServer) CompareTrackingEventOptions(ctx context.Context, req *shippingv1.CompareTrackingEventOptionsRequest) (*shippingv1.CompareTrackingEventOptionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveTrackingEvent(ctx, req.GetContext().GetTenantId(), req.GetTrackingEvent()); err != nil {
		return nil, grpcx.Wrap(err, "save tracking event")
	}
	s.log.InfoContext(ctx, "compare tracking event options", "tenant", req.GetContext().GetTenantId())
	explainShipmentDecisionRequest := &shippingv1.ExplainShipmentDecisionRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
				Locality:    req.GetDetails().GetDestination().GetLocality(),
				Recipient:   req.GetDetails().GetDestination().GetRecipient(),
			},
			ShipmentId: req.GetTrackingEvent().GetSubject().GetShipmentId(),
		},
	}
	explainShipmentDecisionReply, err := s.shipmentValidationPort.ExplainShipmentDecision(ctx, explainShipmentDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentValidationService/ExplainShipmentDecision")
	}
	return &shippingv1.CompareTrackingEventOptionsResponse{
		Result: &commonv1.OperationResult{
			Message: explainShipmentDecisionReply.GetOutcome().GetMessage(),
			Details: explainShipmentDecisionReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		TrackingEvent: &shippingv1.TrackingEvent{
			TrackingEventId: req.GetTrackingEventId(),
			Attributes:      req.GetTrackingEvent().GetAttributes(),
		},
	}, nil
}

// RecalculateTrackingEvents recalculates a tracking event.
func (s *TrackingEventCalculationServer) RecalculateTrackingEvents(ctx context.Context, req *shippingv1.RecalculateTrackingEventsRequest) (*shippingv1.RecalculateTrackingEventsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecalculateTrackingEvents", "error", err)
		return nil, err
	}
	if err := s.store.SaveTrackingEvent(ctx, req.GetContext().GetTenantId(), req.GetTrackingEvent()); err != nil {
		return nil, grpcx.Wrap(err, "save tracking event")
	}
	s.log.InfoContext(ctx, "recalculate tracking events", "tenant", req.GetContext().GetTenantId())
	var envelope shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	exportJobOpeningsStream, err := s.jobOpeningQueryClient.ExportJobOpenings(ctx, &recruitingv1.ExportJobOpeningsRequest{
		Context: req.GetContext(),
		Filter: &recruitingv1.ExportJobOpeningsRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: req.GetTrackingEvent().GetOccurredAt(),
			Query:        envelope.GetDisplayName(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.recruiting.v1.JobOpeningQueryService/ExportJobOpenings")
	}
	for {
		received, err := exportJobOpeningsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.recruiting.v1.JobOpeningQueryService/ExportJobOpenings")
		}
		_ = received
	}
	var envelope2 shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	previewShipmentRequest := &shippingv1.PreviewShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{Recipient: req.GetDetails().GetDestination().GetRecipient()},
			Counterparty: &shippingv1.ParcelRef{
				DisplayName: req.GetTrackingEvent().GetSubject().GetDisplayName(),
			},
		},
		ShipmentId: envelope2.GetDisplayName(),
	}
	previewShipmentReply := new(shippingv1.PreviewShipmentResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", shippingShipmentValidationServiceName, "PreviewShipment"), previewShipmentRequest, previewShipmentReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentValidationService/PreviewShipment")
	}
	var envelope3 shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope3); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	normalizeShipmentRequest := &shippingv1.NormalizeShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			Audit: &commonv1.AuditInfo{
				Etag: previewShipmentReply.GetShipment().GetAudit().GetEtag(),
			},
		},
		ShipmentId: envelope3.GetDisplayName(),
	}
	normalizeShipmentReply := new(shippingv1.NormalizeShipmentResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", shippingShipmentValidationServiceName, "NormalizeShipment"), normalizeShipmentRequest, normalizeShipmentReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentValidationService/NormalizeShipment")
	}
	return &shippingv1.RecalculateTrackingEventsResponse{
		TrackingEvent: &shippingv1.TrackingEvent{
			CorrelationId:   previewShipmentReply.GetResult().GetCorrelationId(),
			TrackingEventId: req.GetTrackingEventId(),
			Subject:         req.GetTrackingEvent().GetSubject(),
		},
		Result: &commonv1.OperationResult{
			Status:        previewShipmentReply.GetOutcome().GetStatus(),
			CorrelationId: normalizeShipmentReply.GetResult().GetCorrelationId(),
		},
	}, nil
}

// ExplainTrackingEventCalculation explains a tracking event.
func (s *TrackingEventCalculationServer) ExplainTrackingEventCalculation(ctx context.Context, req *shippingv1.ExplainTrackingEventCalculationRequest) (*shippingv1.ExplainTrackingEventCalculationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveTrackingEvent(ctx, req.GetContext().GetTenantId(), req.GetTrackingEvent()); err != nil {
		return nil, grpcx.Wrap(err, "save tracking event")
	}
	s.log.InfoContext(ctx, "explain tracking event calculation", "tenant", req.GetContext().GetTenantId())
	var envelope shippingv1.ShipmentRef
	if err := req.GetTrackingEvent().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	streamSearchIndexNoticesStream, err := s.searchIndexNotificationClient.StreamSearchIndexNotices(ctx, &searchv1.StreamSearchIndexNoticesRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			Email:            req.GetDetails().GetContact().GetEmail(),
			Name:             req.GetDetails().GetContact().GetName(),
		},
		Filter: &searchv1.StreamSearchIndexNoticesRequest_Filter{Query: envelope.GetDisplayName()},
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
	return &shippingv1.ExplainTrackingEventCalculationResponse{
		TrackingEvent: &shippingv1.TrackingEvent{
			TrackingEventId: req.GetTrackingEventId(),
			Kind:            req.GetTrackingEvent().GetKind(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetTrackingEventRates returns a tracking event.
func (s *TrackingEventCalculationServer) GetTrackingEventRates(ctx context.Context, req *shippingv1.GetTrackingEventRatesRequest) (*shippingv1.GetTrackingEventRatesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tracking_event", req.GetTrackingEventId(), "get_tracking_event_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get tracking event rates", "tenant", req.GetContext().GetTenantId())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "shipping.get_tracking_event_rates.validate_seller",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"seller.relationship_manager.user_id": req.GetContext().GetActor().GetUserId(),
			"details.contact.email":               req.GetContext().GetActor().GetEmail(),
			"context.tenant_id":                   req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	recalculateParcelsRequest := &shippingv1.RecalculateParcelsRequest{Context: req.GetContext()}
	recalculateParcelsReply, err := grpcx.Call(ctx, s.parcelCalculationClient.RecalculateParcels, recalculateParcelsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/RecalculateParcels")
	}
	return &shippingv1.GetTrackingEventRatesResponse{
		TrackingEvent: &shippingv1.TrackingEvent{
			Subject: &shippingv1.ShipmentRef{
				DisplayName: recalculateParcelsReply.GetParcel().GetOwner().GetDisplayName(),
			},
			Measurements: []*shippingv1.TrackingEventMeasurement{&shippingv1.TrackingEventMeasurement{
				Unit: recalculateParcelsReply.GetParcel().GetDimensions().GetUnit(),
			}},
			TrackingEventId: req.GetTrackingEventId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// trackingEventCalculationServerEstimateTrackingEventExplainSellerCalculationStep carries the request of one step of
// EstimateTrackingEvent and, once it ran, its reply.
type trackingEventCalculationServerEstimateTrackingEventExplainSellerCalculationStep struct {
	request *sellersv1.ExplainSellerCalculationRequest
	reply   *sellersv1.ExplainSellerCalculationResponse
}

func (s *TrackingEventCalculationServer) runEstimateTrackingEventExplainSellerCalculation(ctx context.Context, step *trackingEventCalculationServerEstimateTrackingEventExplainSellerCalculationStep) error {
	reply, err := s.sellerCalculationClient.ExplainSellerCalculation(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
