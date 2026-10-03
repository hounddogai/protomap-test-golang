package shipping

import (
	"context"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// ManifestCalculationServer implements `acme.shipping.v1.ManifestCalculationService`.
// Computes the figures of a manifest, such as totals, taxes, and estimates.
type ManifestCalculationServer struct {
	shippingv1.UnimplementedManifestCalculationServiceServer
	store                                *Store
	log                                  *slog.Logger
	clients                              *registry.Registry
	giftCardBatchQueryClient             giftcardsv1.GiftCardBatchQueryServiceClient
	metricSeriesValidationClient         metricsv1.MetricSeriesValidationServiceClient
	shipmentCalculationClient            shippingv1.ShipmentCalculationServiceClient
	sendRoleTestNoticeFn                 func(context.Context, *identityv1.SendRoleTestNoticeRequest, ...grpc.CallOption) (*identityv1.SendRoleTestNoticeResponse, error)
	tierCalculationClient                loyaltyv1.TierCalculationServiceClient
	trackingEventCalculationClient       shippingv1.TrackingEventCalculationServiceClient
	documentValidationClient             documentsv1.DocumentValidationServiceClient
	restockDecisionCalculationClient     returnsv1.RestockDecisionCalculationServiceClient
	carrierRateCalculationClient         shippingv1.CarrierRateCalculationServiceClient
	giftCardTransactionCalculationClient giftcardsv1.GiftCardTransactionCalculationServiceClient
	conn                                 *grpc.ClientConn
}

// NewManifestCalculationServer wires a ManifestCalculationServer.
func NewManifestCalculationServer(deps platform.Deps) *ManifestCalculationServer {
	roleNotification := identityv1.NewRoleNotificationServiceClient(deps.Conn)
	return &ManifestCalculationServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.shipping.v1.ManifestCalculationService"),
		clients:                              deps.Clients,
		giftCardBatchQueryClient:             giftcardsv1.NewGiftCardBatchQueryServiceClient(deps.Conn),
		metricSeriesValidationClient:         metricsv1.NewMetricSeriesValidationServiceClient(deps.Conn),
		shipmentCalculationClient:            shippingv1.NewShipmentCalculationServiceClient(deps.Conn),
		sendRoleTestNoticeFn:                 roleNotification.SendRoleTestNotice,
		tierCalculationClient:                loyaltyv1.NewTierCalculationServiceClient(deps.Conn),
		trackingEventCalculationClient:       shippingv1.NewTrackingEventCalculationServiceClient(deps.Conn),
		documentValidationClient:             documentsv1.NewDocumentValidationServiceClient(deps.Conn),
		restockDecisionCalculationClient:     returnsv1.NewRestockDecisionCalculationServiceClient(deps.Conn),
		carrierRateCalculationClient:         shippingv1.NewCarrierRateCalculationServiceClient(deps.Conn),
		giftCardTransactionCalculationClient: giftcardsv1.NewGiftCardTransactionCalculationServiceClient(deps.Conn),
		conn:                                 deps.Conn,
	}
}

// CalculateManifest calculates a manifest.
func (s *ManifestCalculationServer) CalculateManifest(ctx context.Context, req *shippingv1.CalculateManifestRequest) (*shippingv1.CalculateManifestResponse, error) {
	if err := s.store.SaveManifest(ctx, req.GetContext().GetTenantId(), req.GetManifest()); err != nil {
		return nil, grpcx.Wrap(err, "save manifest")
	}
	s.log.InfoContext(ctx, "calculate manifest", "tenant", req.GetContext().GetTenantId(), "etag", req.GetManifest().GetAudit().GetEtag())
	giftCardBatchValidation, ok := s.clients.Get("acme.giftcards.v1.GiftCardBatchValidationService").(giftcardsv1.GiftCardBatchValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.giftcards.v1.GiftCardBatchValidationService")
	}
	ctx = tenancy.With(ctx, "bic", req.GetManifest().GetBank().GetBic())
	cloned := proto.Clone(req.GetDetails()).(*shippingv1.CalculateManifestRequest_Details)
	checkGiftCardBatchEligibilityRequest := &giftcardsv1.CheckGiftCardBatchEligibilityRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			BillingAddress: &commonv1.PostalAddress{
				Lines:      req.GetDetails().GetDestination().GetLines(),
				PostalCode: cloned.GetDestination().GetPostalCode(),
			},
			Settlement: &giftcardsv1.GiftCardBatch_Bank{Bank: &giftcardsv1.GiftCardBatchBankSettlement{Bic: tenancy.Value(ctx, "bic")}},
		},
	}
	checkGiftCardBatchEligibilityReply, err := giftCardBatchValidation.CheckGiftCardBatchEligibility(ctx, checkGiftCardBatchEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchValidationService/CheckGiftCardBatchEligibility")
	}
	return &shippingv1.CalculateManifestResponse{
		Manifest: &shippingv1.Manifest{
			Audit: &commonv1.AuditInfo{
				Etag:      checkGiftCardBatchEligibilityReply.GetGiftCardBatch().GetAudit().GetEtag(),
				CreatedAt: checkGiftCardBatchEligibilityReply.GetGiftCardBatch().GetAudit().GetCreatedAt(),
			},
			ManifestId: req.GetManifestId(),
		},
		Breakdown: &shippingv1.CalculateManifestResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EstimateManifest estimates a manifest.
func (s *ManifestCalculationServer) EstimateManifest(ctx context.Context, req *shippingv1.EstimateManifestRequest) (*shippingv1.EstimateManifestResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveManifest(ctx, req.GetContext().GetTenantId(), req.GetManifest()); err != nil {
		return nil, grpcx.Wrap(err, "save manifest")
	}
	s.log.InfoContext(ctx, "estimate manifest", "tenant", req.GetContext().GetTenantId(), "etag", req.GetManifest().GetAudit().GetEtag())
	parcelCalculation, err := registry.Lookup[shippingv1.ParcelCalculationServiceClient](s.clients, "acme.shipping.v1.ParcelCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	cloned := proto.Clone(req.GetManifest()).(*shippingv1.Manifest)
	compareParcelOptionsRequest := &shippingv1.CompareParcelOptionsRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Locator: &shippingv1.Parcel_Address{Address: &commonv1.PostalAddress{CountryCode: cloned.GetBillingAddress().GetCountryCode()}},
		},
	}
	compareParcelOptionsReply, err := parcelCalculation.CompareParcelOptions(ctx, compareParcelOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/CompareParcelOptions")
	}
	suggestGiftCardBatchesReply, err := s.giftCardBatchQueryClient.SuggestGiftCardBatches(ctx, &giftcardsv1.SuggestGiftCardBatchesRequest{
		Context: req.GetContext(),
		Filter: &giftcardsv1.SuggestGiftCardBatchesRequest_Filter{
			Region: compareParcelOptionsReply.GetParcel().GetAddress().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchQueryService/SuggestGiftCardBatches")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*metricsv1.ValidateMetricSeriesResponse, len(req.GetManifest().GetLines()))
	for i, line := range req.GetManifest().GetLines() {
		group.Go(func() error {
			reply, err := s.metricSeriesValidationClient.ValidateMetricSeries(groupCtx, &metricsv1.ValidateMetricSeriesRequest{
				Context: req.GetContext(),
				MetricSeries: &metricsv1.MetricSeries{
					Components: map[string]*metricsv1.MetricSeriesComponent{"primary": &metricsv1.MetricSeriesComponent{
						Description: line.GetDescription(),
						Quantity:    line.GetQuantity(),
					}},
					Owner: &identityv1.UserRef{Email: req.GetManifest().GetCounterparty().GetEmail()},
				},
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesValidationService/ValidateMetricSeries")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &shippingv1.EstimateManifestResponse{
		Manifest: &shippingv1.Manifest{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: compareParcelOptionsReply.GetParcel().GetAudit().GetUpdatedBy(),
			},
			ManifestId: req.GetManifestId(),
			Metadata:   req.GetManifest().GetMetadata(),
		},
		Result: &commonv1.OperationResult{
			Status:       suggestGiftCardBatchesReply.GetResult().GetStatus(),
			ReferenceIds: suggestGiftCardBatchesReply.GetResult().GetReferenceIds(),
		},
	}, nil
}

// QuoteManifest quotes a manifest.
func (s *ManifestCalculationServer) QuoteManifest(ctx context.Context, req *shippingv1.QuoteManifestRequest) (*shippingv1.QuoteManifestResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveManifest(ctx, req.GetContext().GetTenantId(), req.GetManifest()); err != nil {
		return nil, grpcx.Wrap(err, "save manifest")
	}
	s.log.InfoContext(ctx, "quote manifest", "tenant", req.GetContext().GetTenantId(), "etag", req.GetManifest().GetAudit().GetEtag())
	jobs := make(chan *shippingv1.ManifestAttachment)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for attachment := range jobs {
				if _, err := s.shipmentCalculationClient.CalculateShipment(workCtx, &shippingv1.CalculateShipmentRequest{
					Context: req.GetContext(),
					Shipment: &shippingv1.Shipment{
						Attachments: []*shippingv1.ShipmentAttachment{&shippingv1.ShipmentAttachment{Uri: attachment.GetUri()}},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetManifest().GetAttachments() {
		if workCtx.Err() != nil {
			break
		}
		select {
		case jobs <- queued:
		case <-workCtx.Done():
		}
	}
	close(jobs)
	wg.Wait()
	stopWork()
	close(failures)
	if err := <-failures; err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/CalculateShipment")
	}
	packed, err := anypb.New(req.GetManifest())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	sendRoleTestNoticeRequest := &identityv1.SendRoleTestNoticeRequest{
		Context: req.GetContext(),
		Details: &identityv1.SendRoleTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetManifest().GetCounterparty().GetEmail()},
			Payload:   packed,
		},
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
	}
	sendRoleTestNoticeReply, err := s.sendRoleTestNoticeFn(ctx, sendRoleTestNoticeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleNotificationService/SendRoleTestNotice")
	}
	return &shippingv1.QuoteManifestResponse{
		Result: &commonv1.OperationResult{
			Message: sendRoleTestNoticeReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Manifest: &shippingv1.Manifest{
			ManifestId: req.GetManifestId(),
			Notes:      req.GetManifest().GetNotes(),
		},
	}, nil
}

// CompareManifestOptions compares a manifest.
func (s *ManifestCalculationServer) CompareManifestOptions(ctx context.Context, req *shippingv1.CompareManifestOptionsRequest) (*shippingv1.CompareManifestOptionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveManifest(ctx, req.GetContext().GetTenantId(), req.GetManifest()); err != nil {
		return nil, grpcx.Wrap(err, "save manifest")
	}
	s.log.InfoContext(ctx, "compare manifest options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetManifest().GetAudit().GetEtag())
	estimateTierRequest := &loyaltyv1.EstimateTierRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.EstimateTierRequest_Details{
			Destination: &commonv1.PostalAddress{Recipient: req.GetDetails().GetDestination().GetRecipient()},
		},
	}
	estimateTierReply, err := grpcx.Call(ctx, s.tierCalculationClient.EstimateTier, estimateTierRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierCalculationService/EstimateTier")
	}
	ctx = tenancy.With(ctx, "region", req.GetManifest().GetBillingAddress().GetRegion())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded shippingv1.CompareManifestOptionsRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	step := &manifestCalculationServerCompareManifestOptionsEstimateTrackingEventStep{request: &shippingv1.EstimateTrackingEventRequest{
		Context: req.GetContext(),
		Details: &shippingv1.EstimateTrackingEventRequest_Details{
			Destination: &commonv1.PostalAddress{
				Lines:       req.GetDetails().GetDestination().GetLines(),
				Region:      tenancy.Value(ctx, "region"),
				CountryCode: (&decoded).GetDestination().GetCountryCode(),
			},
		},
		TrackingEvent: &shippingv1.TrackingEvent{
			Subject: &shippingv1.ShipmentRef{
				DisplayName: req.GetManifest().GetCounterparty().GetDisplayName(),
			},
		},
	}}
	if err := s.runCompareManifestOptionsEstimateTrackingEvent(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventCalculationService/EstimateTrackingEvent")
	}
	estimateTrackingEventReply := step.reply
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*documentsv1.ValidateDocumentResponse, len(req.GetManifest().GetLines()))
	for i, line := range req.GetManifest().GetLines() {
		group.Go(func() error {
			reply, err := s.documentValidationClient.ValidateDocument(groupCtx, &documentsv1.ValidateDocumentRequest{
				Context: req.GetContext(),
				Document: &documentsv1.Document{
					Lines: []*documentsv1.DocumentLine{&documentsv1.DocumentLine{
						Tax: &documentsv1.DocumentLineTax{
							Rate:   line.GetTax().GetRate(),
							Amount: line.GetAmount(),
						},
						Amount:    line.GetTax().GetAmount(),
						UnitPrice: line.GetUnitPrice(),
					}},
				},
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentValidationService/ValidateDocument")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &shippingv1.CompareManifestOptionsResponse{
		Result: &commonv1.OperationResult{
			Details: estimateTierReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Breakdown: &shippingv1.CompareManifestOptionsResponse_Breakdown{Total: estimateTrackingEventReply.GetBreakdown().GetTotal()},
		Manifest: &shippingv1.Manifest{
			ManifestId:   req.GetManifestId(),
			Counterparty: req.GetManifest().GetCounterparty(),
		},
	}, nil
}

// RecalculateManifests recalculates a manifest.
func (s *ManifestCalculationServer) RecalculateManifests(ctx context.Context, req *shippingv1.RecalculateManifestsRequest) (*shippingv1.RecalculateManifestsResponse, error) {
	if err := s.store.SaveManifest(ctx, req.GetContext().GetTenantId(), req.GetManifest()); err != nil {
		return nil, grpcx.Wrap(err, "save manifest")
	}
	s.log.InfoContext(ctx, "recalculate manifests", "tenant", req.GetContext().GetTenantId(), "etag", req.GetManifest().GetAudit().GetEtag())
	estimateRestockDecisionRequest := &returnsv1.EstimateRestockDecisionRequest{
		Context: req.GetContext(),
		Details: &returnsv1.EstimateRestockDecisionRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region: req.GetManifest().GetBillingAddress().GetRegion(),
				Lines:  req.GetDetails().GetDestination().GetLines(),
			},
		},
		RestockDecision: &returnsv1.RestockDecision{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetManifest().GetAudit().GetUpdatedBy(),
				CreatedBy: req.GetManifest().GetAudit().GetCreatedBy(),
			},
		},
	}
	send := s.estimateRestockDecisionSender(estimateRestockDecisionRequest)
	estimateRestockDecisionReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionCalculationService/EstimateRestockDecision")
	}
	ctx = tenancy.With(ctx, "iban", req.GetManifest().GetBank().GetIban())
	compareShipmentOptionsReply, err := s.shipmentCalculationClient.CompareShipmentOptions(ctx, &shippingv1.CompareShipmentOptionsRequest{
		Context: req.GetContext(),
		Details: &shippingv1.CompareShipmentOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{Lines: req.GetDetails().GetDestination().GetLines()},
		},
		Shipment: &shippingv1.Shipment{
			Settlement: &shippingv1.Shipment_Bank{Bank: &shippingv1.ShipmentBankSettlement{Iban: tenancy.Value(ctx, "iban")}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: estimateRestockDecisionReply.GetRestockDecision().GetAudit().GetCreatedAt(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/CompareShipmentOptions")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*shippingv1.CompareCarrierRateOptionsResponse, len(req.GetManifest().GetLines()))
	for i, line := range req.GetManifest().GetLines() {
		group.Go(func() error {
			reply, err := s.carrierRateCalculationClient.CompareCarrierRateOptions(groupCtx, &shippingv1.CompareCarrierRateOptionsRequest{
				Context: req.GetContext(),
				CarrierRate: &shippingv1.CarrierRate{
					Action:      &shippingv1.CarrierRate_Discount{Discount: &shippingv1.CarrierRateDiscountAction{Amount: line.GetTax().GetAmount()}},
					Description: line.GetDescription(),
				},
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateCalculationService/CompareCarrierRateOptions")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	recalculateGiftCardTransactionsRequest := &giftcardsv1.RecalculateGiftCardTransactionsRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.RecalculateGiftCardTransactionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: compareShipmentOptionsReply.GetShipment().GetBillingAddress().GetCountryCode(),
				Lines:       req.GetDetails().GetDestination().GetLines(),
			},
		},
	}
	send2 := s.recalculateGiftCardTransactionsSender(recalculateGiftCardTransactionsRequest)
	recalculateGiftCardTransactionsReply, err := send2(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/RecalculateGiftCardTransactions")
	}
	return &shippingv1.RecalculateManifestsResponse{
		Manifest: &shippingv1.Manifest{
			Total: estimateRestockDecisionReply.GetBreakdown().GetTotal(),
			BillingAddress: &commonv1.PostalAddress{
				Region: compareShipmentOptionsReply.GetShipment().GetBillingAddress().GetRegion(),
			},
			ManifestId:   req.GetManifestId(),
			Counterparty: req.GetManifest().GetCounterparty(),
		},
		Result: &commonv1.OperationResult{
			Details: recalculateGiftCardTransactionsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// SimulateManifest simulates a manifest.
func (s *ManifestCalculationServer) SimulateManifest(ctx context.Context, req *shippingv1.SimulateManifestRequest) (*shippingv1.SimulateManifestResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SimulateManifest", "error", err)
		return nil, err
	}
	if err := s.store.SaveManifest(ctx, req.GetContext().GetTenantId(), req.GetManifest()); err != nil {
		return nil, grpcx.Wrap(err, "save manifest")
	}
	s.log.InfoContext(ctx, "simulate manifest", "tenant", req.GetContext().GetTenantId(), "etag", req.GetManifest().GetAudit().GetEtag())
	recalculateShipmentsRequest := &shippingv1.RecalculateShipmentsRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
			},
		},
	}
	recalculateShipmentsReply := new(shippingv1.RecalculateShipmentsResponse)
	if err := grpcx.Invoke(ctx, s.conn, shippingv1.ShipmentCalculationService_RecalculateShipments_FullMethodName, recalculateShipmentsRequest, recalculateShipmentsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/RecalculateShipments")
	}
	return &shippingv1.SimulateManifestResponse{
		Manifest: &shippingv1.Manifest{
			Audit: &commonv1.AuditInfo{
				CreatedAt: recalculateShipmentsReply.GetShipment().GetAudit().GetCreatedAt(),
			},
			ManifestId: req.GetManifestId(),
			IssuedAt:   req.GetManifest().GetIssuedAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// manifestCalculationServerCompareManifestOptionsEstimateTrackingEventStep carries the request of one step of
// CompareManifestOptions and, once it ran, its reply.
type manifestCalculationServerCompareManifestOptionsEstimateTrackingEventStep struct {
	request *shippingv1.EstimateTrackingEventRequest
	reply   *shippingv1.EstimateTrackingEventResponse
}

func (s *ManifestCalculationServer) runCompareManifestOptionsEstimateTrackingEvent(ctx context.Context, step *manifestCalculationServerCompareManifestOptionsEstimateTrackingEventStep) error {
	reply, err := s.trackingEventCalculationClient.EstimateTrackingEvent(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// estimateRestockDecisionSender returns a function that sends one request to
// acme.returns.v1.RestockDecisionCalculationService/EstimateRestockDecision.
func (s *ManifestCalculationServer) estimateRestockDecisionSender(request *returnsv1.EstimateRestockDecisionRequest) func(context.Context) (*returnsv1.EstimateRestockDecisionResponse, error) {
	return func(ctx context.Context) (*returnsv1.EstimateRestockDecisionResponse, error) {
		return s.restockDecisionCalculationClient.EstimateRestockDecision(ctx, request)
	}
}

// recalculateGiftCardTransactionsSender returns a function that sends one request to
// acme.giftcards.v1.GiftCardTransactionCalculationService/RecalculateGiftCardTransactions.
func (s *ManifestCalculationServer) recalculateGiftCardTransactionsSender(request *giftcardsv1.RecalculateGiftCardTransactionsRequest) func(context.Context) (*giftcardsv1.RecalculateGiftCardTransactionsResponse, error) {
	return func(ctx context.Context) (*giftcardsv1.RecalculateGiftCardTransactionsResponse, error) {
		return s.giftCardTransactionCalculationClient.RecalculateGiftCardTransactions(ctx, request)
	}
}
