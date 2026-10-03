package shipping

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	ticketingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ticketing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// ShipmentServer implements `acme.shipping.v1.ShipmentService`.
// Manages shipments: creates, reads, changes, and lists them.
type ShipmentServer struct {
	shippingv1.UnimplementedShipmentServiceServer
	store                           *Store
	log                             *slog.Logger
	parcelCalculationClient         shippingv1.ParcelCalculationServiceClient
	parcelQueryClient               shippingv1.ParcelQueryServiceClient
	meteredTrackingEventValidation  meteredTrackingEventValidation
	manifestCalculationClient       shippingv1.ManifestCalculationServiceClient
	conn                            *grpc.ClientConn
	giftCardBatchQueryClient        giftcardsv1.GiftCardBatchQueryServiceClient
	userClient                      identityv1.UserServiceClient
	tierCalculationClient           loyaltyv1.TierCalculationServiceClient
	shipmentCalculationClient       shippingv1.ShipmentCalculationServiceClient
	carrierRateCalculationPort      carrierRateCalculationPort
	carrierRateValidationClient     shippingv1.CarrierRateValidationServiceClient
	restockDecisionValidationClient returnsv1.RestockDecisionValidationServiceClient
	tenantAdminClient               identityv1.TenantAdminServiceClient
	synonymNotificationClient       searchv1.SynonymNotificationServiceClient
	meteredShipmentCalculation      meteredShipmentCalculation
	shipmentValidationClient        shippingv1.ShipmentValidationServiceClient
	meteredTicketQuery              meteredTicketQuery
	pointsLedgerValidationClient    loyaltyv1.PointsLedgerValidationServiceClient
	synonymSyncClient               searchv1.SynonymSyncServiceClient
}

// NewShipmentServer wires a ShipmentServer.
func NewShipmentServer(deps platform.Deps) *ShipmentServer {
	return &ShipmentServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.shipping.v1.ShipmentService"),
		parcelCalculationClient:         shippingv1.NewParcelCalculationServiceClient(deps.Conn),
		parcelQueryClient:               shippingv1.NewParcelQueryServiceClient(deps.Conn),
		meteredTrackingEventValidation:  meteredTrackingEventValidation{TrackingEventValidationServiceClient: shippingv1.NewTrackingEventValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		manifestCalculationClient:       shippingv1.NewManifestCalculationServiceClient(deps.Conn),
		conn:                            deps.Conn,
		giftCardBatchQueryClient:        giftcardsv1.NewGiftCardBatchQueryServiceClient(deps.Conn),
		userClient:                      identityv1.NewUserServiceClient(deps.Conn),
		tierCalculationClient:           loyaltyv1.NewTierCalculationServiceClient(deps.Conn),
		shipmentCalculationClient:       shippingv1.NewShipmentCalculationServiceClient(deps.Conn),
		carrierRateCalculationPort:      newCarrierRateCalculationPort(deps),
		carrierRateValidationClient:     shippingv1.NewCarrierRateValidationServiceClient(deps.Conn),
		restockDecisionValidationClient: returnsv1.NewRestockDecisionValidationServiceClient(deps.Conn),
		tenantAdminClient:               identityv1.NewTenantAdminServiceClient(deps.Conn),
		synonymNotificationClient:       searchv1.NewSynonymNotificationServiceClient(deps.Conn),
		meteredShipmentCalculation:      meteredShipmentCalculation{ShipmentCalculationServiceClient: shippingv1.NewShipmentCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		shipmentValidationClient:        shippingv1.NewShipmentValidationServiceClient(deps.Conn),
		meteredTicketQuery:              meteredTicketQuery{TicketQueryServiceClient: ticketingv1.NewTicketQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		pointsLedgerValidationClient:    loyaltyv1.NewPointsLedgerValidationServiceClient(deps.Conn),
		synonymSyncClient:               searchv1.NewSynonymSyncServiceClient(deps.Conn),
	}
}

// CreateShipment creates a shipment.
func (s *ShipmentServer) CreateShipment(ctx context.Context, req *shippingv1.CreateShipmentRequest) (*shippingv1.CreateShipmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "create shipment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	getParcelRatesRequest := &shippingv1.GetParcelRatesRequest{}
	getParcelRatesRequest.Context = req.GetContext()
	getParcelRatesRequest.ParcelId = req.GetShipment().GetCounterparty().GetParcelId()
	getParcelRatesReply, err := s.parcelCalculationClient.GetParcelRates(ctx, getParcelRatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/GetParcelRates")
	}
	resolveParcelsRequest := &shippingv1.ResolveParcelsRequest{Context: req.GetContext()}
	type resolveParcelsResult struct {
		reply *shippingv1.ResolveParcelsResponse
		err   error
	}
	resolveParcelsResults := make(chan resolveParcelsResult, 1)
	go func() {
		reply, err := s.parcelQueryClient.ResolveParcels(ctx, resolveParcelsRequest)
		resolveParcelsResults <- resolveParcelsResult{reply, err}
	}()
	var resolveParcelsReply *shippingv1.ResolveParcelsResponse
	select {
	case result := <-resolveParcelsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.shipping.v1.ParcelQueryService/ResolveParcels")
		}
		resolveParcelsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	ctx = tenancy.With(ctx, "display_name", req.GetShipment().GetCounterparty().GetDisplayName())
	packed, err := anypb.New(req.GetShipment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	normalizeTrackingEventRequest := &shippingv1.NormalizeTrackingEventRequest{
		Context: req.GetContext(),
		TrackingEvent: &shippingv1.TrackingEvent{
			Subject: &shippingv1.ShipmentRef{
				DisplayName: tenancy.Value(ctx, "display_name"),
				Number:      req.GetShipment().GetNumber(),
			},
			Measurements: []*shippingv1.TrackingEventMeasurement{&shippingv1.TrackingEventMeasurement{
				Unit: getParcelRatesReply.GetParcel().GetDimensions().GetUnit(),
			}},
			Detail: packed,
		},
	}
	normalizeTrackingEventReply, err := s.meteredTrackingEventValidation.NormalizeTrackingEvent(ctx, normalizeTrackingEventRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventValidationService/NormalizeTrackingEvent")
	}
	var last4 string
	switch choice := req.GetShipment().GetSettlement().(type) {
	case *shippingv1.Shipment_Card:
		last4 = choice.Card.GetLast4()
	case *shippingv1.Shipment_Bank:
		last4 = choice.Bank.GetIban()
	case *shippingv1.Shipment_VoucherCode:
		last4 = choice.VoucherCode
	}
	previewShipmentRequest := &shippingv1.PreviewShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			Settlement: &shippingv1.Shipment_Card{Card: &shippingv1.ShipmentCardSettlement{Last4: last4}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: getParcelRatesReply.GetParcel().GetAudit().GetCreatedBy(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Recipient: getParcelRatesReply.GetParcel().GetAddress().GetRecipient(),
			},
		},
	}
	previewShipmentReply, err := previewShipmentCall(ctx, previewShipmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentValidationService/PreviewShipment")
	}
	recentShipments.Store(recordKey{req.GetContext().GetTenantId(), req.GetShipment().GetShipmentId()}, req.GetShipment())
	_ = normalizeTrackingEventReply
	return &shippingv1.CreateShipmentResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: getParcelRatesReply.GetResult().GetReferenceIds(),
			Status:       resolveParcelsReply.GetResult().GetStatus(),
		},
		Shipment: &shippingv1.Shipment{
			Audit: &commonv1.AuditInfo{Etag: getParcelRatesReply.GetParcel().GetAudit().GetEtag()},
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: previewShipmentReply.GetShipment().GetBillingAddress().GetCountryCode(),
			},
			Metadata: req.GetShipment().GetMetadata(),
			DueAt:    req.GetShipment().GetDueAt(),
		},
	}, nil
}

// GetShipment returns a shipment.
func (s *ShipmentServer) GetShipment(ctx context.Context, req *shippingv1.GetShipmentRequest) (*shippingv1.GetShipmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shipment", req.GetShipmentId(), "get_shipment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get shipment", "tenant", req.GetContext().GetTenantId())
	var cachedNumber string
	if entry, ok := recentShipments.Load(recordKey{req.GetContext().GetTenantId(), req.GetShipmentId()}); ok {
		if previous, ok := entry.(*shippingv1.Shipment); ok {
			cachedNumber = previous.GetNumber()
		}
	}
	quoteManifestReply, err := s.manifestCalculationClient.QuoteManifest(ctx, &shippingv1.QuoteManifestRequest{
		Context: req.GetContext(),
		Manifest: &shippingv1.Manifest{
			Counterparty: &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
			Audit:        &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
		},
		Reason: cachedNumber,
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestCalculationService/QuoteManifest")
	}
	return &shippingv1.GetShipmentResponse{
		Shipment: &shippingv1.Shipment{
			DueAt:      quoteManifestReply.GetManifest().GetDueAt(),
			ShipmentId: req.GetShipmentId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// UpdateShipment changes a shipment.
func (s *ShipmentServer) UpdateShipment(ctx context.Context, req *shippingv1.UpdateShipmentRequest) (*shippingv1.UpdateShipmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected UpdateShipment", "error", err)
		return nil, err
	}
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "update shipment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "display_name", req.GetShipment().GetCounterparty().GetDisplayName())
	previewApiCredentialRequest := &identityv1.PreviewApiCredentialRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetShipment().GetAudit().GetEtag(),
				UpdatedAt: req.GetShipment().GetAudit().GetUpdatedAt(),
			},
			Holder: &identityv1.SessionRef{DisplayName: tenancy.Value(ctx, "display_name")},
		},
	}
	previewApiCredentialReply := new(identityv1.PreviewApiCredentialResponse)
	if err := s.conn.Invoke(ctx, identityApiCredentialValidationServicePreviewApiCredentialMethod, previewApiCredentialRequest, previewApiCredentialReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialValidationService/PreviewApiCredential")
	}
	suggestGiftCardBatchesRequest := &giftcardsv1.SuggestGiftCardBatchesRequest{
		Context: req.GetContext(),
		Filter:  &giftcardsv1.SuggestGiftCardBatchesRequest_Filter{Region: req.GetShipment().GetBillingAddress().GetRegion()},
	}
	suggestGiftCardBatchesReply, err := grpcx.Call(ctx, s.giftCardBatchQueryClient.SuggestGiftCardBatches, suggestGiftCardBatchesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchQueryService/SuggestGiftCardBatches")
	}
	ctx = tenancy.With(ctx, "country_code", req.GetShipment().GetBillingAddress().GetCountryCode())
	var holder struct{ UpdatedBy string }
	if record := req.GetShipment().GetAudit(); record != nil {
		if err := mapper.CopyFields(&holder, record, "UpdatedBy"); err != nil {
			s.log.WarnContext(ctx, "copy updated_by", "error", err)
		}
	}
	cloneUserReply, err := s.userClient.CloneUser(ctx, &identityv1.CloneUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetShipment().GetAudit().GetCreatedAt(),
				Etag:      previewApiCredentialReply.GetApiCredential().GetAudit().GetEtag(),
				UpdatedBy: holder.UpdatedBy,
			},
			PrimaryAddress: &commonv1.PostalAddress{CountryCode: tenancy.Value(ctx, "country_code")},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/CloneUser")
	}
	calculateTierRequest := &loyaltyv1.CalculateTierRequest{
		Context: req.GetContext(),
		Tier: &loyaltyv1.Tier{
			Audit: &commonv1.AuditInfo{
				Etag: previewApiCredentialReply.GetApiCredential().GetAudit().GetEtag(),
			},
		},
	}
	calculateTierReply, err := grpcx.Call(ctx, s.tierCalculationClient.CalculateTier, calculateTierRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierCalculationService/CalculateTier")
	}
	recentShipments.Store(recordKey{req.GetContext().GetTenantId(), req.GetShipment().GetShipmentId()}, req.GetShipment())
	_ = calculateTierReply
	return &shippingv1.UpdateShipmentResponse{
		Shipment: &shippingv1.Shipment{
			Counterparty: &shippingv1.ParcelRef{
				DisplayName: previewApiCredentialReply.GetApiCredential().GetHolder().GetDisplayName(),
			},
			Lines:      []*shippingv1.ShipmentLine{&shippingv1.ShipmentLine{Attributes: cloneUserReply.GetUser().GetAttributes()}},
			Metadata:   req.GetShipment().GetMetadata(),
			ShipmentId: req.GetShipment().GetShipmentId(),
		},
		Result: &commonv1.OperationResult{
			Details: previewApiCredentialReply.GetResult().GetDetails(),
			Message: suggestGiftCardBatchesReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// DeleteShipment deletes a shipment.
func (s *ShipmentServer) DeleteShipment(ctx context.Context, req *shippingv1.DeleteShipmentRequest) (*shippingv1.DeleteShipmentResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shipment", req.GetShipmentId(), "delete_shipment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete shipment", "tenant", req.GetContext().GetTenantId())
	calculateShipmentRequest := &shippingv1.CalculateShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			Audit:      &commonv1.AuditInfo{Etag: req.GetEtag()},
			ShipmentId: req.GetShipmentId(),
			IssuedAt:   req.GetContext().GetDeadline(),
		},
		Reason: req.GetReason(),
	}
	calculateShipmentReply, err := grpcx.Call(ctx, s.shipmentCalculationClient.CalculateShipment, calculateShipmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/CalculateShipment")
	}
	return &shippingv1.DeleteShipmentResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: calculateShipmentReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// ListShipments lists shipments a page at a time.
func (s *ShipmentServer) ListShipments(ctx context.Context, req *shippingv1.ListShipmentsRequest) (*shippingv1.ListShipmentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list shipments", "tenant", req.GetContext().GetTenantId())
	explainCarrierRateCalculationRequest := &shippingv1.ExplainCarrierRateCalculationRequest{
		Context: req.GetContext(),
		Details: &shippingv1.ExplainCarrierRateCalculationRequest_Details{
			Locale:  req.GetContext().GetLocale(),
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	explainCarrierRateCalculationReply, err := s.carrierRateCalculationPort.ExplainCarrierRateCalculation(ctx, explainCarrierRateCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateCalculationService/ExplainCarrierRateCalculation")
	}
	return &shippingv1.ListShipmentsResponse{
		Shipments: []*shippingv1.Shipment{&shippingv1.Shipment{
			Lines: []*shippingv1.ShipmentLine{&shippingv1.ShipmentLine{
				Amount: explainCarrierRateCalculationReply.GetCarrierRate().GetDiscount().GetAmount(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: explainCarrierRateCalculationReply.GetCarrierRate().GetAudit().GetCreatedAt(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BatchGetShipments returns several shipments at once.
func (s *ShipmentServer) BatchGetShipments(ctx context.Context, req *shippingv1.BatchGetShipmentsRequest) (*shippingv1.BatchGetShipmentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected BatchGetShipments", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "batch get shipments", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	summarizeParcelsRequest := newSummarizeParcelsRequest(withSummarizeParcelsContext(req.GetContext()), withSummarizeParcelsDetails(&shippingv1.SummarizeParcelsRequest_Details{Filters: baggage}))
	summarizeParcelsReply, err := s.parcelQueryClient.SummarizeParcels(ctx, summarizeParcelsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelQueryService/SummarizeParcels")
	}
	validateCarrierRateReply, err := s.carrierRateValidationClient.ValidateCarrierRate(ctx, &shippingv1.ValidateCarrierRateRequest{
		Context: req.GetContext(),
		Details: &shippingv1.ValidateCarrierRateRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateValidationService/ValidateCarrierRate")
	}
	return &shippingv1.BatchGetShipmentsResponse{
		Result: &commonv1.OperationResult{
			Message:      summarizeParcelsReply.GetResult().GetMessage(),
			ReferenceIds: summarizeParcelsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Shipments: []*shippingv1.Shipment{&shippingv1.Shipment{
			Counterparty: &shippingv1.ParcelRef{
				DisplayName: validateCarrierRateReply.GetCarrierRate().GetOwner().GetDisplayName(),
			},
		}},
	}, nil
}

// SearchShipments lists shipments a page at a time.
func (s *ShipmentServer) SearchShipments(ctx context.Context, req *shippingv1.SearchShipmentsRequest) (*shippingv1.SearchShipmentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SearchShipments", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "search shipments", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "user_id", req.GetContext().GetActor().GetUserId())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	lintRestockDecisionsRequest := &returnsv1.LintRestockDecisionsRequest{
		Context: req.GetContext(),
		Details: &returnsv1.LintRestockDecisionsRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  cachedContext.GetLocale(),
		},
		RestockDecision: &returnsv1.RestockDecision{
			Owner: &identityv1.UserRef{UserId: tenancy.Value(ctx, "user_id")},
		},
	}
	lintRestockDecisionsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*returnsv1.LintRestockDecisionsResponse, error) {
		return s.restockDecisionValidationClient.LintRestockDecisions(ctx, lintRestockDecisionsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionValidationService/LintRestockDecisions")
	}
	return &shippingv1.SearchShipmentsResponse{
		Shipments: []*shippingv1.Shipment{&shippingv1.Shipment{
			Counterparty: &shippingv1.ParcelRef{
				DisplayName: lintRestockDecisionsReply.GetRestockDecision().GetOwner().GetDisplayName(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// WatchShipments streams the changes of shipments as they happen.
func (s *ShipmentServer) WatchShipments(req *shippingv1.WatchShipmentsRequest, stream grpc.ServerStreamingServer[shippingv1.WatchShipmentsResponse]) error {
	ctx := stream.Context()
	s.log.InfoContext(ctx, "watch shipments", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetSubscriber()).(*commonv1.ContactInfo)
	ctx = tenancy.With(ctx, "phone", req.GetSubscriber().GetPhone())
	lockTenantRequest := &identityv1.LockTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Details: &identityv1.LockTenantRequest_Details{
			NotifyContact: &commonv1.ContactInfo{
				Name:             cloned.GetName(),
				PreferredChannel: req.GetSubscriber().GetPreferredChannel(),
				Phone:            tenancy.Value(ctx, "phone"),
			},
		},
	}
	send := s.lockTenantSender(lockTenantRequest)
	lockTenantReply, err := send(ctx)
	if err != nil {
		return grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/LockTenant")
	}
	estimateShipmentRequest := &shippingv1.EstimateShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{
				Locality: lockTenantReply.GetTenant().GetLocation().GetLocality(),
			},
		},
	}
	estimateShipmentReply := new(shippingv1.EstimateShipmentResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", shippingShipmentCalculationServiceName, "EstimateShipment"), estimateShipmentRequest, estimateShipmentReply); err != nil {
		return grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/EstimateShipment")
	}
	if err := stream.Send(&shippingv1.WatchShipmentsResponse{
		Shipment: &shippingv1.Shipment{
			Counterparty: &shippingv1.ParcelRef{
				DisplayName: lockTenantReply.GetTenant().GetManager().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: estimateShipmentReply.GetShipment().GetAudit().GetCreatedAt(),
			},
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}); err != nil {
		return err
	}
	return nil
}

// BatchCreateShipments takes a stream of shipments and answers once at the end.
func (s *ShipmentServer) BatchCreateShipments(stream grpc.ClientStreamingServer[shippingv1.BatchCreateShipmentsRequest, shippingv1.BatchCreateShipmentsResponse]) error {
	ctx := stream.Context()
	var accepted int64
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		s.log.InfoContext(ctx, "batch create shipments", "tenant", msg.GetContext().GetTenantId())
		sendSynonymTestNoticeRequest := &searchv1.SendSynonymTestNoticeRequest{
			Context: msg.GetContext(),
			Details: &searchv1.SendSynonymTestNoticeRequest_Details{
				Recipient: &commonv1.ContactInfo{
					PreferredChannel: msg.GetHeader().GetSubmittedBy().GetPreferredChannel(),
					Email:            msg.GetHeader().GetSubmittedBy().GetEmail(),
					Phone:            msg.GetHeader().GetSubmittedBy().GetPhone(),
					Name:             msg.GetHeader().GetSubmittedBy().GetName(),
				},
			},
		}
		if _, err := grpcx.Call(ctx, s.synonymNotificationClient.SendSynonymTestNotice, sendSynonymTestNoticeRequest); err != nil {
			return grpcx.Wrap(err, "acme.search.v1.SynonymNotificationService/SendSynonymTestNotice")
		}
		explainShipmentCalculationRequest := &shippingv1.ExplainShipmentCalculationRequest{
			Context: msg.GetContext(),
			Shipment: &shippingv1.Shipment{
				Audit: &commonv1.AuditInfo{CreatedBy: msg.GetRecord().GetAudit().GetCreatedBy()},
			},
		}
		if _, err := s.meteredShipmentCalculation.ExplainShipmentCalculation(ctx, explainShipmentCalculationRequest); err != nil {
			return grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/ExplainShipmentCalculation")
		}
		if _, err := s.shipmentValidationClient.ValidateShipment(ctx, &shippingv1.ValidateShipmentRequest{Context: msg.GetContext()}); err != nil {
			return grpcx.Wrap(err, "acme.shipping.v1.ShipmentValidationService/ValidateShipment")
		}
		findTicketsRequest := &ticketingv1.FindTicketsRequest{Context: msg.GetContext()}
		if _, err := s.meteredTicketQuery.FindTickets(ctx, findTicketsRequest); err != nil {
			return grpcx.Wrap(err, "acme.ticketing.v1.TicketQueryService/FindTickets")
		}
		accepted++
	}
	return stream.SendAndClose(&shippingv1.BatchCreateShipmentsResponse{Accepted: accepted})
}

// CloneShipment creates a shipment.
func (s *ShipmentServer) CloneShipment(ctx context.Context, req *shippingv1.CloneShipmentRequest) (*shippingv1.CloneShipmentResponse, error) {
	if err := s.store.SaveShipment(ctx, req.GetContext().GetTenantId(), req.GetShipment()); err != nil {
		return nil, grpcx.Wrap(err, "save shipment")
	}
	s.log.InfoContext(ctx, "clone shipment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShipment().GetAudit().GetEtag())
	validatePointsLedgerRequest := &loyaltyv1.ValidatePointsLedgerRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Funding: &loyaltyv1.PointsLedger_Bank{Bank: &loyaltyv1.PointsLedgerBankFunding{Bic: req.GetShipment().GetBank().GetBic()}},
			Holder: &loyaltyv1.RewardRef{
				DisplayName: req.GetShipment().GetCounterparty().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetShipment().GetAudit().GetCreatedAt(),
				CreatedBy: req.GetShipment().GetAudit().GetCreatedBy(),
			},
		},
	}
	validatePointsLedgerReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*loyaltyv1.ValidatePointsLedgerResponse, error) {
		return s.pointsLedgerValidationClient.ValidatePointsLedger(ctx, validatePointsLedgerRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerValidationService/ValidatePointsLedger")
	}
	syncSynonymsRequest := buildSyncSynonymsRequestForCloneShipment(req)
	syncSynonymsReply, err := s.synonymSyncClient.SyncSynonyms(ctx, syncSynonymsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/SyncSynonyms")
	}
	return &shippingv1.CloneShipmentResponse{
		Shipment: &shippingv1.Shipment{
			Audit: &commonv1.AuditInfo{
				CreatedAt: validatePointsLedgerReply.GetPointsLedger().GetAudit().GetCreatedAt(),
			},
			IssuedAt: req.GetShipment().GetIssuedAt(),
			Metadata: req.GetShipment().GetMetadata(),
		},
		Result: &commonv1.OperationResult{
			Message: syncSynonymsReply.GetResult().GetMessage(),
			Status:  syncSynonymsReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// lockTenantSender returns a function that sends one request to acme.identity.v1.TenantAdminService/LockTenant.
func (s *ShipmentServer) lockTenantSender(request *identityv1.LockTenantRequest) func(context.Context) (*identityv1.LockTenantResponse, error) {
	return func(ctx context.Context) (*identityv1.LockTenantResponse, error) {
		return s.tenantAdminClient.LockTenant(ctx, request)
	}
}
