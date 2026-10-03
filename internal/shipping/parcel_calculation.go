package shipping

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	compliancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/compliance/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ParcelCalculationServer implements `acme.shipping.v1.ParcelCalculationService`.
// Computes the figures of a parcel, such as totals, taxes, and estimates.
type ParcelCalculationServer struct {
	shippingv1.UnsafeParcelCalculationServiceServer
	store                             *Store
	log                               *slog.Logger
	shipmentCalculationClient         shippingv1.ShipmentCalculationServiceClient
	shipmentCalculationClientFallback shippingv1.ShipmentCalculationServiceClient
	sessionValidationClient           identityv1.SessionValidationServiceClient
	scoreSessionFn                    func(context.Context, *identityv1.ScoreSessionRequest, ...grpc.CallOption) (*identityv1.ScoreSessionResponse, error)
	bus                               *eventbus.Bus
	shipmentCalculationPort           shipmentCalculationPort
	conn                              *grpc.ClientConn
	userValidationClient              identityv1.UserValidationServiceClient
	sellerListingValidationOnce       sync.Once
	sellerListingValidationCached     sellersv1.SellerListingValidationServiceClient
	complianceCaseCalculationClient   compliancev1.ComplianceCaseCalculationServiceClient
	productReviewQueryClient          reviewsv1.ProductReviewQueryServiceClient
}

// NewParcelCalculationServer wires a ParcelCalculationServer.
func NewParcelCalculationServer(deps platform.Deps) *ParcelCalculationServer {
	sessionValidation := identityv1.NewSessionValidationServiceClient(deps.Conn)
	return &ParcelCalculationServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.shipping.v1.ParcelCalculationService"),
		shipmentCalculationClient:         shippingv1.NewShipmentCalculationServiceClient(deps.Conn),
		shipmentCalculationClientFallback: shippingv1.NewShipmentCalculationServiceClient(deps.Conn),
		sessionValidationClient:           identityv1.NewSessionValidationServiceClient(deps.Conn),
		scoreSessionFn:                    sessionValidation.ScoreSession,
		bus:                               deps.Bus,
		shipmentCalculationPort:           newShipmentCalculationPort(deps),
		conn:                              deps.Conn,
		userValidationClient:              identityv1.NewUserValidationServiceClient(deps.Conn),
		complianceCaseCalculationClient:   compliancev1.NewComplianceCaseCalculationServiceClient(deps.Conn),
		productReviewQueryClient:          reviewsv1.NewProductReviewQueryServiceClient(deps.Conn),
	}
}

// CalculateParcel calculates a parcel.
func (s *ParcelCalculationServer) CalculateParcel(ctx context.Context, req *shippingv1.CalculateParcelRequest) (*shippingv1.CalculateParcelResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CalculateParcel", "error", err)
		return nil, err
	}
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "calculate parcel", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *shippingv1.QuoteShipmentRequest, ...grpc.CallOption) (*shippingv1.QuoteShipmentResponse, error){
		"primary":  s.shipmentCalculationClient.QuoteShipment,
		"fallback": s.shipmentCalculationClientFallback.QuoteShipment,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	detailsCache := cache.New[string, *shippingv1.CalculateParcelRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	quoteShipmentRequest := &shippingv1.QuoteShipmentRequest{
		Context: req.GetContext(),
		Details: &shippingv1.QuoteShipmentRequest_Details{
			Destination: &commonv1.PostalAddress{
				Lines:      req.GetParcel().GetAddress().GetLines(),
				Recipient:  cachedDetails.GetDestination().GetRecipient(),
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
			},
		},
		Shipment: &shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{PostalCode: req.GetParcel().GetAddress().GetPostalCode()},
		},
	}
	quoteShipmentReply, err := route(ctx, quoteShipmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/QuoteShipment")
	}
	ctx = tenancy.With(ctx, "email", req.GetParcel().GetOwner().GetEmail())
	previewSessionRequest := &identityv1.PreviewSessionRequest{}
	previewSessionRequest.Context = req.GetContext()
	previewSessionRequest.Session = &identityv1.Session{
		Assignee: &identityv1.UserRef{
			DisplayName: req.GetParcel().GetOwner().GetDisplayName(),
			Email:       tenancy.Value(ctx, "email"),
		},
		Audit: &commonv1.AuditInfo{
			CreatedBy: quoteShipmentReply.GetShipment().GetAudit().GetCreatedBy(),
			UpdatedAt: req.GetParcel().GetAudit().GetUpdatedAt(),
		},
	}
	previewSessionReply, err := s.sessionValidationClient.PreviewSession(ctx, previewSessionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionValidationService/PreviewSession")
	}
	scoreSessionRequest := &identityv1.ScoreSessionRequest{
		Context: req.GetContext(),
		Details: &identityv1.ScoreSessionRequest_Details{
			Destination: &commonv1.PostalAddress{Locality: req.GetDetails().GetDestination().GetLocality()},
		},
	}
	scoreSessionReply, err := s.scoreSessionFn(ctx, scoreSessionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionValidationService/ScoreSession")
	}
	return &shippingv1.CalculateParcelResponse{
		Breakdown: &shippingv1.CalculateParcelResponse_Breakdown{
			Total:        quoteShipmentReply.GetShipment().GetTotal(),
			CurrencyCode: req.GetDetails().GetCurrencyCode(),
		},
		Parcel: &shippingv1.Parcel{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: previewSessionReply.GetSession().GetAudit().GetUpdatedBy(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: previewSessionReply.GetSession().GetCustomer().GetDisplayName(),
			},
			Tags:     scoreSessionReply.GetSession().GetTags(),
			ParcelId: req.GetParcelId(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: scoreSessionReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// EstimateParcel estimates a parcel.
func (s *ParcelCalculationServer) EstimateParcel(ctx context.Context, req *shippingv1.EstimateParcelRequest) (*shippingv1.EstimateParcelResponse, error) {
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "estimate parcel", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	estimateShipmentRequest := buildEstimateShipmentRequestForEstimateParcel(req)
	estimateShipmentReply, err := s.shipmentCalculationClient.EstimateShipment(ctx, estimateShipmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/EstimateShipment")
	}
	simulateShipmentReply, err := s.shipmentCalculationClient.SimulateShipment(ctx, &shippingv1.SimulateShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{Locality: req.GetDetails().GetDestination().GetLocality()},
			Settlement: &shippingv1.Shipment_Card{Card: &shippingv1.ShipmentCardSettlement{
				HolderName: estimateShipmentReply.GetShipment().GetCard().GetHolderName(),
			}},
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetParcel().GetAudit().GetUpdatedBy()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/SimulateShipment")
	}
	recalculateShipmentsRequest := &shippingv1.RecalculateShipmentsRequest{}
	recalculateShipmentsRequest.Context = req.GetContext()
	recalculateShipmentsRequest.Shipment = &shippingv1.Shipment{
		Audit: &commonv1.AuditInfo{UpdatedAt: req.GetParcel().GetAudit().GetUpdatedAt()},
	}
	recalculateShipmentsReply, err := s.shipmentCalculationClient.RecalculateShipments(ctx, recalculateShipmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/RecalculateShipments")
	}
	return &shippingv1.EstimateParcelResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  estimateShipmentReply.GetResult().GetReferenceIds(),
			CorrelationId: simulateShipmentReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Parcel: &shippingv1.Parcel{
			Code:     estimateShipmentReply.GetShipment().GetCounterparty().GetCode(),
			ParcelId: req.GetParcelId(),
			Name:     req.GetParcel().GetName(),
		},
		Breakdown: &shippingv1.EstimateParcelResponse_Breakdown{
			CurrencyCode: recalculateShipmentsReply.GetBreakdown().GetCurrencyCode(),
		},
	}, nil
}

// QuoteParcel quotes a parcel.
func (s *ParcelCalculationServer) QuoteParcel(ctx context.Context, req *shippingv1.QuoteParcelRequest) (*shippingv1.QuoteParcelResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected QuoteParcel", "error", err)
		return nil, err
	}
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "quote parcel", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "shipping.quote_parcel.update_collection",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"collection.children.children.location.recipient": req.GetDetails().GetDestination().GetRecipient(),
			"collection.children.children.location.region":    req.GetParcel().GetAddress().GetRegion(),
			"collection.children.children.manager.email":      req.GetParcel().GetOwner().GetEmail(),
			"collection.children.location.locality":           req.GetParcel().GetAddress().GetLocality(),
			"context.tenant_id":                               req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	explainShipmentCalculationRequest := buildExplainShipmentCalculationRequestForQuoteParcel(req)
	explainShipmentCalculationReply, err := s.shipmentCalculationClient.ExplainShipmentCalculation(ctx, explainShipmentCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/ExplainShipmentCalculation")
	}
	return &shippingv1.QuoteParcelResponse{
		Parcel: &shippingv1.Parcel{
			Code: explainShipmentCalculationReply.GetShipment().GetCounterparty().GetCode(),
			Audit: &commonv1.AuditInfo{
				UpdatedBy: explainShipmentCalculationReply.GetShipment().GetAudit().GetUpdatedBy(),
			},
			ParcelId: req.GetParcelId(),
			Name:     req.GetParcel().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CompareParcelOptions compares a parcel.
func (s *ParcelCalculationServer) CompareParcelOptions(ctx context.Context, req *shippingv1.CompareParcelOptionsRequest) (*shippingv1.CompareParcelOptionsResponse, error) {
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "compare parcel options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	compareShipmentOptionsRequest := buildCompareShipmentOptionsRequestForCompareParcelOptions(req)
	compareShipmentOptionsReply, err := s.shipmentCalculationClient.CompareShipmentOptions(ctx, compareShipmentOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/CompareShipmentOptions")
	}
	return &shippingv1.CompareParcelOptionsResponse{
		Parcel: &shippingv1.Parcel{
			Owner: &identityv1.UserRef{
				DisplayName: compareShipmentOptionsReply.GetShipment().GetCounterparty().GetDisplayName(),
			},
			ParcelId: req.GetParcelId(),
			Name:     req.GetParcel().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecalculateParcels recalculates a parcel.
func (s *ParcelCalculationServer) RecalculateParcels(ctx context.Context, req *shippingv1.RecalculateParcelsRequest) (*shippingv1.RecalculateParcelsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecalculateParcels", "error", err)
		return nil, err
	}
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "recalculate parcels", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "recipient", req.GetParcel().GetAddress().GetRecipient())
	estimateShipmentRequest := &shippingv1.EstimateShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
			},
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetParcel().GetAudit().GetUpdatedBy()},
		},
		Details: &shippingv1.EstimateShipmentRequest_Details{
			Destination: &commonv1.PostalAddress{Recipient: tenancy.Value(ctx, "recipient")},
		},
	}
	estimateShipmentReply, err := s.shipmentCalculationPort.EstimateShipment(ctx, estimateShipmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/EstimateShipment")
	}
	quoteShipmentRequest := &shippingv1.QuoteShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{Lines: req.GetParcel().GetAddress().GetLines()},
		},
		Details: &shippingv1.QuoteShipmentRequest_Details{
			Destination: &commonv1.PostalAddress{Locality: req.GetParcel().GetAddress().GetLocality()},
		},
	}
	quoteShipmentReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*shippingv1.QuoteShipmentResponse, error) {
		return s.shipmentCalculationClient.QuoteShipment(ctx, quoteShipmentRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/QuoteShipment")
	}
	return &shippingv1.RecalculateParcelsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: estimateShipmentReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Breakdown: &shippingv1.RecalculateParcelsResponse_Breakdown{
			Total:        estimateShipmentReply.GetShipment().GetTotal(),
			CurrencyCode: quoteShipmentReply.GetBreakdown().GetCurrencyCode(),
		},
		Parcel: &shippingv1.Parcel{
			Audit:    &commonv1.AuditInfo{Etag: quoteShipmentReply.GetShipment().GetAudit().GetEtag()},
			ParcelId: req.GetParcelId(),
			Code:     req.GetParcel().GetCode(),
		},
	}, nil
}

// SimulateParcel simulates a parcel.
func (s *ParcelCalculationServer) SimulateParcel(ctx context.Context, req *shippingv1.SimulateParcelRequest) (*shippingv1.SimulateParcelResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "simulate parcel", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "recipient", req.GetParcel().GetAddress().GetRecipient())
	quoteShipmentRequest := &shippingv1.QuoteShipmentRequest{
		Context: req.GetContext(),
		Details: &shippingv1.QuoteShipmentRequest_Details{
			Destination: &commonv1.PostalAddress{
				Lines:     req.GetDetails().GetDestination().GetLines(),
				Recipient: tenancy.Value(ctx, "recipient"),
			},
		},
		Shipment: &shippingv1.Shipment{
			Audit:          &commonv1.AuditInfo{UpdatedAt: req.GetParcel().GetAudit().GetUpdatedAt()},
			BillingAddress: &commonv1.PostalAddress{PostalCode: req.GetParcel().GetAddress().GetPostalCode()},
		},
	}
	quoteShipmentReply := new(shippingv1.QuoteShipmentResponse)
	if err := s.conn.Invoke(ctx, shippingShipmentCalculationServiceQuoteShipmentMethod, quoteShipmentRequest, quoteShipmentReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/QuoteShipment")
	}
	checkUserEligibilityRequest := &identityv1.CheckUserEligibilityRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetParcel().GetAudit().GetCreatedAt()},
			RelationshipManager: &identityv1.UserRef{
				DisplayName: quoteShipmentReply.GetShipment().GetCounterparty().GetDisplayName(),
			},
		},
	}
	checkUserEligibilityReply, err := grpcx.Call(ctx, s.userValidationClient.CheckUserEligibility, checkUserEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/CheckUserEligibility")
	}
	scoreSellerListingRequest := &sellersv1.ScoreSellerListingRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Locator: &sellersv1.SellerListing_Address{Address: &commonv1.PostalAddress{
				Recipient:  req.GetDetails().GetDestination().GetRecipient(),
				PostalCode: quoteShipmentReply.GetShipment().GetBillingAddress().GetPostalCode(),
			}},
		},
	}
	scoreSellerListingReply, err := s.sellerListingValidationLazy().ScoreSellerListing(ctx, scoreSellerListingRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingValidationService/ScoreSellerListing")
	}
	quoteComplianceCaseRequest := &compliancev1.QuoteComplianceCaseRequest{
		Context: req.GetContext(),
		Details: &compliancev1.QuoteComplianceCaseRequest_Details{
			Destination: &commonv1.PostalAddress{Recipient: req.GetDetails().GetDestination().GetRecipient()},
		},
		ComplianceCase: &compliancev1.ComplianceCase{
			Assignee: &identityv1.UserRef{UserId: req.GetParcel().GetOwner().GetUserId()},
			Audit:    &commonv1.AuditInfo{Etag: req.GetParcel().GetAudit().GetEtag()},
		},
	}
	quoteComplianceCaseReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*compliancev1.QuoteComplianceCaseResponse, error) {
		return s.complianceCaseCalculationClient.QuoteComplianceCase(ctx, quoteComplianceCaseRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.compliance.v1.ComplianceCaseCalculationService/QuoteComplianceCase")
	}
	return &shippingv1.SimulateParcelResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: quoteShipmentReply.GetResult().GetCorrelationId(),
			Status:        quoteShipmentReply.GetResult().GetStatus(),
			ReferenceIds:  quoteComplianceCaseReply.GetResult().GetReferenceIds(),
		},
		Parcel: &shippingv1.Parcel{
			Audit: &commonv1.AuditInfo{
				CreatedAt: checkUserEligibilityReply.GetUser().GetAudit().GetCreatedAt(),
				UpdatedBy: scoreSellerListingReply.GetSellerListing().GetAudit().GetUpdatedBy(),
			},
			ParcelId: req.GetParcelId(),
			Owner:    req.GetParcel().GetOwner(),
		},
		Breakdown: &shippingv1.SimulateParcelResponse_Breakdown{Total: quoteComplianceCaseReply.GetBreakdown().GetTotal()},
	}, nil
}

// ExplainParcelCalculation explains a parcel.
func (s *ParcelCalculationServer) ExplainParcelCalculation(ctx context.Context, req *shippingv1.ExplainParcelCalculationRequest) (*shippingv1.ExplainParcelCalculationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainParcelCalculation", "error", err)
		return nil, err
	}
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "explain parcel calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	var region string
	switch choice := req.GetParcel().GetLocator().(type) {
	case *shippingv1.Parcel_Address:
		region = choice.Address.GetRegion()
	case *shippingv1.Parcel_FacilityCode:
		region = choice.FacilityCode
	}
	exportProductReviewsStream, err := s.productReviewQueryClient.ExportProductReviews(ctx, &reviewsv1.ExportProductReviewsRequest{
		Context: req.GetContext(),
		Filter: &reviewsv1.ExportProductReviewsRequest_Filter{
			Region:       region,
			CreatedAfter: req.GetParcel().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetParcel().GetOwner().GetUserId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewQueryService/ExportProductReviews")
	}
	for {
		received, err := exportProductReviewsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewQueryService/ExportProductReviews")
		}
		_ = received
	}
	compareShipmentOptionsReply, err := s.shipmentCalculationClient.CompareShipmentOptions(ctx, &shippingv1.CompareShipmentOptionsRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			Lines: []*shippingv1.ShipmentLine{&shippingv1.ShipmentLine{
				Description: req.GetParcel().GetComponents()["primary"].GetDescription(),
			}},
			BillingAddress: &commonv1.PostalAddress{
				Locality: req.GetParcel().GetAddress().GetLocality(),
				Region:   req.GetParcel().GetAddress().GetRegion(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/CompareShipmentOptions")
	}
	estimateShipmentRequest := &shippingv1.EstimateShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			Counterparty: &shippingv1.ParcelRef{DisplayName: req.GetParcel().GetOwner().GetDisplayName()},
		},
	}
	estimateShipmentReply, err := shippingv1.ShipmentCalculationServiceClient.EstimateShipment(s.shipmentCalculationClient, ctx, estimateShipmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/EstimateShipment")
	}
	var postalCode string
	switch choice2 := req.GetParcel().GetLocator().(type) {
	case *shippingv1.Parcel_Address:
		postalCode = choice2.Address.GetPostalCode()
	case *shippingv1.Parcel_FacilityCode:
		postalCode = choice2.FacilityCode
	}
	scoreUserRequest := &identityv1.ScoreUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			PrimaryAddress: &commonv1.PostalAddress{PostalCode: postalCode},
		},
	}
	scoreUserReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*identityv1.ScoreUserResponse, error) {
		return s.userValidationClient.ScoreUser(ctx, scoreUserRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/ScoreUser")
	}
	_ = scoreUserReply
	return &shippingv1.ExplainParcelCalculationResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: compareShipmentOptionsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Parcel: &shippingv1.Parcel{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: estimateShipmentReply.GetShipment().GetAudit().GetUpdatedAt(),
			},
			ParcelId: req.GetParcelId(),
			Name:     req.GetParcel().GetName(),
		},
	}, nil
}

// GetParcelRates returns a parcel.
func (s *ParcelCalculationServer) GetParcelRates(ctx context.Context, req *shippingv1.GetParcelRatesRequest) (*shippingv1.GetParcelRatesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "parcel", req.GetParcelId(), "get_parcel_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get parcel rates", "tenant", req.GetContext().GetTenantId())
	simulateShipmentRequest := &shippingv1.SimulateShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			Counterparty: &shippingv1.ParcelRef{ParcelId: req.GetParcelId()},
			Lines:        []*shippingv1.ShipmentLine{&shippingv1.ShipmentLine{Attributes: req.GetContext().GetBaggage()}},
		},
	}
	simulateShipmentReply := new(shippingv1.SimulateShipmentResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.shipping.v1.ShipmentCalculationService/SimulateShipment", simulateShipmentRequest, simulateShipmentReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/SimulateShipment")
	}
	return &shippingv1.GetParcelRatesResponse{
		Parcel: &shippingv1.Parcel{
			Code:     simulateShipmentReply.GetShipment().GetCounterparty().GetCode(),
			ParcelId: req.GetParcelId(),
		},
		Result: &commonv1.OperationResult{Status: simulateShipmentReply.GetResult().GetStatus()},
	}, nil
}

// sellerListingValidationLazy dials acme.sellers.v1.SellerListingValidationService the first time a handler needs it.
func (s *ParcelCalculationServer) sellerListingValidationLazy() sellersv1.SellerListingValidationServiceClient {
	s.sellerListingValidationOnce.Do(func() { s.sellerListingValidationCached = sellersv1.NewSellerListingValidationServiceClient(s.conn) })
	return s.sellerListingValidationCached
}
