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
	recommendationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/recommendations/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// CarrierRateCalculationServer implements `acme.shipping.v1.CarrierRateCalculationService`.
// Computes the figures of a carrier rate, such as totals, taxes, and estimates.
type CarrierRateCalculationServer struct {
	shippingv1.UnimplementedCarrierRateCalculationServiceServer
	store                        *Store
	log                          *slog.Logger
	clients                      *registry.Registry
	synonymValidationClient      searchv1.SynonymValidationServiceClient
	shipmentCalculationClient    shippingv1.ShipmentCalculationServiceClient
	parcelCalculationPort        parcelCalculationPort
	conn                         *grpc.ClientConn
	parcelCalculationClient      shippingv1.ParcelCalculationServiceClient
	recommendationSetQueryClient recommendationsv1.RecommendationSetQueryServiceClient
	userClient                   identityv1.UserServiceClient
}

// NewCarrierRateCalculationServer wires a CarrierRateCalculationServer.
func NewCarrierRateCalculationServer(deps platform.Deps) *CarrierRateCalculationServer {
	return &CarrierRateCalculationServer{
		store:                        NewStore(deps.DB),
		log:                          deps.Logger.With("service", "acme.shipping.v1.CarrierRateCalculationService"),
		clients:                      deps.Clients,
		synonymValidationClient:      searchv1.NewSynonymValidationServiceClient(deps.Conn),
		shipmentCalculationClient:    shippingv1.NewShipmentCalculationServiceClient(deps.Conn),
		parcelCalculationPort:        newParcelCalculationPort(deps),
		conn:                         deps.Conn,
		parcelCalculationClient:      shippingv1.NewParcelCalculationServiceClient(deps.Conn),
		recommendationSetQueryClient: recommendationsv1.NewRecommendationSetQueryServiceClient(deps.Conn),
		userClient:                   identityv1.NewUserServiceClient(deps.Conn),
	}
}

// CalculateCarrierRate calculates a carrier rate.
func (s *CarrierRateCalculationServer) CalculateCarrierRate(ctx context.Context, req *shippingv1.CalculateCarrierRateRequest) (*shippingv1.CalculateCarrierRateResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CalculateCarrierRate", "error", err)
		return nil, err
	}
	if err := s.store.SaveCarrierRate(ctx, req.GetContext().GetTenantId(), req.GetCarrierRate()); err != nil {
		return nil, grpcx.Wrap(err, "save carrier rate")
	}
	s.log.InfoContext(ctx, "calculate carrier rate", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCarrierRate().GetAudit().GetEtag())
	giftCardTransactionValidation, ok := s.clients.Get("acme.giftcards.v1.GiftCardTransactionValidationService").(giftcardsv1.GiftCardTransactionValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.giftcards.v1.GiftCardTransactionValidationService")
	}
	previewGiftCardTransactionRequest := &giftcardsv1.PreviewGiftCardTransactionRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: req.GetCarrierRate().GetOwner().GetDisplayName(),
			},
		},
		Details: &giftcardsv1.PreviewGiftCardTransactionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetCarrierRate().GetOwner().GetEmail()},
		},
	}
	previewGiftCardTransactionReply, err := giftCardTransactionValidation.PreviewGiftCardTransaction(ctx, previewGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionValidationService/PreviewGiftCardTransaction")
	}
	return &shippingv1.CalculateCarrierRateResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: previewGiftCardTransactionReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		CarrierRate: &shippingv1.CarrierRate{
			CarrierRateId: req.GetCarrierRateId(),
			Priority:      req.GetCarrierRate().GetPriority(),
		},
	}, nil
}

// EstimateCarrierRate estimates a carrier rate.
func (s *CarrierRateCalculationServer) EstimateCarrierRate(ctx context.Context, req *shippingv1.EstimateCarrierRateRequest) (*shippingv1.EstimateCarrierRateResponse, error) {
	if err := s.store.SaveCarrierRate(ctx, req.GetContext().GetTenantId(), req.GetCarrierRate()); err != nil {
		return nil, grpcx.Wrap(err, "save carrier rate")
	}
	s.log.InfoContext(ctx, "estimate carrier rate", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCarrierRate().GetAudit().GetEtag())
	explainSynonymDecisionRequest := buildExplainSynonymDecisionRequestForEstimateCarrierRate(req)
	explainSynonymDecisionReply, err := s.synonymValidationClient.ExplainSynonymDecision(ctx, explainSynonymDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/ExplainSynonymDecision")
	}
	explainShipmentCalculationRequest := &shippingv1.ExplainShipmentCalculationRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetCarrierRate().GetAudit().GetCreatedAt()},
		},
	}
	explainShipmentCalculationReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*shippingv1.ExplainShipmentCalculationResponse, error) {
		return s.shipmentCalculationClient.ExplainShipmentCalculation(ctx, explainShipmentCalculationRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/ExplainShipmentCalculation")
	}
	encoded, err := protojson.Marshal(req.GetCarrierRate())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded shippingv1.CarrierRate
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	calculateParcelRequest := &shippingv1.CalculateParcelRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Locator: &shippingv1.Parcel_Address{Address: &commonv1.PostalAddress{
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
			}},
			Owner: &identityv1.UserRef{
				DisplayName: explainShipmentCalculationReply.GetShipment().GetCounterparty().GetDisplayName(),
				Email:       (&decoded).GetOwner().GetEmail(),
			},
		},
	}
	calculateParcelReply, err := s.parcelCalculationPort.CalculateParcel(ctx, calculateParcelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/CalculateParcel")
	}
	return &shippingv1.EstimateCarrierRateResponse{
		CarrierRate: &shippingv1.CarrierRate{
			Audit: &commonv1.AuditInfo{
				Etag:      explainSynonymDecisionReply.GetSynonym().GetAudit().GetEtag(),
				UpdatedBy: explainShipmentCalculationReply.GetShipment().GetAudit().GetUpdatedBy(),
				CreatedBy: calculateParcelReply.GetParcel().GetAudit().GetCreatedBy(),
			},
			CarrierRateId: req.GetCarrierRateId(),
			Priority:      req.GetCarrierRate().GetPriority(),
		},
		Result: &commonv1.OperationResult{
			Message: explainShipmentCalculationReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// QuoteCarrierRate quotes a carrier rate.
func (s *CarrierRateCalculationServer) QuoteCarrierRate(ctx context.Context, req *shippingv1.QuoteCarrierRateRequest) (*shippingv1.QuoteCarrierRateResponse, error) {
	if err := s.store.SaveCarrierRate(ctx, req.GetContext().GetTenantId(), req.GetCarrierRate()); err != nil {
		return nil, grpcx.Wrap(err, "save carrier rate")
	}
	s.log.InfoContext(ctx, "quote carrier rate", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCarrierRate().GetAudit().GetEtag())
	estimateInspectionRequest := &returnsv1.EstimateInspectionRequest{
		Context: req.GetContext(),
		Inspection: &returnsv1.Inspection{
			Origin: &returnsv1.Inspection_Customer{Customer: &customersv1.CustomerRef{Email: req.GetCarrierRate().GetOwner().GetEmail()}},
		},
		Details: &returnsv1.EstimateInspectionRequest_Details{
			Destination: &commonv1.PostalAddress{Lines: req.GetDetails().GetDestination().GetLines()},
		},
	}
	estimateInspectionReply := new(returnsv1.EstimateInspectionResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", returnsInspectionCalculationServiceName, "EstimateInspection"), estimateInspectionRequest, estimateInspectionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionCalculationService/EstimateInspection")
	}
	source := req.GetDetails().GetDestination().ProtoReflect()
	countryCode := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("country_code"))).String()
	simulateParcelRequest := &shippingv1.SimulateParcelRequest{}
	simulateParcelRequest.Context = req.GetContext()
	simulateParcelRequest.Parcel = &shippingv1.Parcel{
		Audit: &commonv1.AuditInfo{
			CreatedAt: estimateInspectionReply.GetInspection().GetAudit().GetCreatedAt(),
		},
	}
	simulateParcelRequest.Details = &shippingv1.SimulateParcelRequest_Details{
		Destination: &commonv1.PostalAddress{CountryCode: countryCode},
	}
	simulateParcelReply, err := s.parcelCalculationClient.SimulateParcel(ctx, simulateParcelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/SimulateParcel")
	}
	return &shippingv1.QuoteCarrierRateResponse{
		Result: &commonv1.OperationResult{
			Status:        estimateInspectionReply.GetResult().GetStatus(),
			CorrelationId: estimateInspectionReply.GetResult().GetCorrelationId(),
		},
		Breakdown: &shippingv1.QuoteCarrierRateResponse_Breakdown{
			CurrencyCode: simulateParcelReply.GetBreakdown().GetCurrencyCode(),
		},
		CarrierRate: &shippingv1.CarrierRate{
			CarrierRateId: req.GetCarrierRateId(),
			Priority:      req.GetCarrierRate().GetPriority(),
		},
	}, nil
}

// CompareCarrierRateOptions compares a carrier rate.
func (s *CarrierRateCalculationServer) CompareCarrierRateOptions(ctx context.Context, req *shippingv1.CompareCarrierRateOptionsRequest) (*shippingv1.CompareCarrierRateOptionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCarrierRate(ctx, req.GetContext().GetTenantId(), req.GetCarrierRate()); err != nil {
		return nil, grpcx.Wrap(err, "save carrier rate")
	}
	s.log.InfoContext(ctx, "compare carrier rate options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCarrierRate().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded shippingv1.CompareCarrierRateOptionsRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	detailsCache := cache.New[string, *shippingv1.CompareCarrierRateOptionsRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	calculateParcelRequest := &shippingv1.CalculateParcelRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Audit:   &commonv1.AuditInfo{Etag: req.GetCarrierRate().GetAudit().GetEtag()},
			Locator: &shippingv1.Parcel_Address{Address: &commonv1.PostalAddress{Region: cachedDetails.GetDestination().GetRegion()}},
		},
		Details: &shippingv1.CalculateParcelRequest_Details{
			Destination: &commonv1.PostalAddress{Lines: (&decoded).GetDestination().GetLines()},
		},
	}
	calculateParcelReply := new(shippingv1.CalculateParcelResponse)
	if err := grpcx.Invoke(ctx, s.conn, shippingv1.ParcelCalculationService_CalculateParcel_FullMethodName, calculateParcelRequest, calculateParcelReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/CalculateParcel")
	}
	cloned := proto.Clone(req.GetCarrierRate()).(*shippingv1.CarrierRate)
	checkGiftCardEligibilityRequest := &giftcardsv1.CheckGiftCardEligibilityRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{
				CreatedBy: calculateParcelReply.GetParcel().GetAudit().GetCreatedBy(),
				CreatedAt: req.GetCarrierRate().GetAudit().GetCreatedAt(),
			},
			Limits: []*giftcardsv1.GiftCardLimit{&giftcardsv1.GiftCardLimit{Name: calculateParcelReply.GetParcel().GetName()}},
		},
		Details: &giftcardsv1.CheckGiftCardEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: cloned.GetOwner().GetEmail()},
		},
	}
	checkGiftCardEligibilityReply := new(giftcardsv1.CheckGiftCardEligibilityResponse)
	if err := grpcx.Invoke(ctx, s.conn, giftcardsv1.GiftCardValidationService_CheckGiftCardEligibility_FullMethodName, checkGiftCardEligibilityRequest, checkGiftCardEligibilityReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/CheckGiftCardEligibility")
	}
	return &shippingv1.CompareCarrierRateOptionsResponse{
		CarrierRate: &shippingv1.CarrierRate{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: calculateParcelReply.GetParcel().GetAudit().GetUpdatedBy(),
				Etag:      calculateParcelReply.GetParcel().GetAudit().GetEtag(),
			},
			CarrierRateId: req.GetCarrierRateId(),
			Effective:     req.GetCarrierRate().GetEffective(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: checkGiftCardEligibilityReply.GetResult().GetCorrelationId(),
			Details:       checkGiftCardEligibilityReply.GetResult().GetDetails(),
			Status:        "ok",
		},
	}, nil
}

// RecalculateCarrierRates recalculates a carrier rate.
func (s *CarrierRateCalculationServer) RecalculateCarrierRates(ctx context.Context, req *shippingv1.RecalculateCarrierRatesRequest) (*shippingv1.RecalculateCarrierRatesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCarrierRate(ctx, req.GetContext().GetTenantId(), req.GetCarrierRate()); err != nil {
		return nil, grpcx.Wrap(err, "save carrier rate")
	}
	s.log.InfoContext(ctx, "recalculate carrier rates", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCarrierRate().GetAudit().GetEtag())
	parcelCalculation, err := registry.Lookup[shippingv1.ParcelCalculationServiceClient](s.clients, "acme.shipping.v1.ParcelCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	recalculateParcelsRequest := &shippingv1.RecalculateParcelsRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Locator: &shippingv1.Parcel_Address{Address: &commonv1.PostalAddress{
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
			}},
		},
		Details: &shippingv1.RecalculateParcelsRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
			},
		},
	}
	recalculateParcelsReply, err := parcelCalculation.RecalculateParcels(ctx, recalculateParcelsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/RecalculateParcels")
	}
	return &shippingv1.RecalculateCarrierRatesResponse{
		CarrierRate: &shippingv1.CarrierRate{
			Owner: recalculateParcelsReply.GetParcel().GetOwner(),
			Audit: &commonv1.AuditInfo{
				Etag: recalculateParcelsReply.GetParcel().GetAudit().GetEtag(),
			},
			CarrierRateId: req.GetCarrierRateId(),
			Priority:      req.GetCarrierRate().GetPriority(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SimulateCarrierRate simulates a carrier rate.
func (s *CarrierRateCalculationServer) SimulateCarrierRate(ctx context.Context, req *shippingv1.SimulateCarrierRateRequest) (*shippingv1.SimulateCarrierRateResponse, error) {
	if err := s.store.SaveCarrierRate(ctx, req.GetContext().GetTenantId(), req.GetCarrierRate()); err != nil {
		return nil, grpcx.Wrap(err, "save carrier rate")
	}
	s.log.InfoContext(ctx, "simulate carrier rate", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCarrierRate().GetAudit().GetEtag())
	var holder struct{ Region string }
	if record := req.GetDetails().GetDestination(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Region"); err != nil {
			s.log.WarnContext(ctx, "copy region", "error", err)
		}
	}
	parameters := make(map[string]string, len(req.GetCarrierRate().GetParameters()))
	for key, value := range req.GetCarrierRate().GetParameters() {
		parameters[key] = value
	}
	exportRecommendationSetsStream, err := s.recommendationSetQueryClient.ExportRecommendationSets(ctx, &recommendationsv1.ExportRecommendationSetsRequest{
		Context: req.GetContext(),
		Filter: &recommendationsv1.ExportRecommendationSetsRequest_Filter{
			Region:       holder.Region,
			OwnerId:      req.GetCarrierRate().GetOwner().GetUserId(),
			CreatedAfter: req.GetCarrierRate().GetAudit().GetUpdatedAt(),
			Labels:       parameters,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.recommendations.v1.RecommendationSetQueryService/ExportRecommendationSets")
	}
	for {
		received, err := exportRecommendationSetsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.recommendations.v1.RecommendationSetQueryService/ExportRecommendationSets")
		}
		_ = received
	}
	return &shippingv1.SimulateCarrierRateResponse{
		CarrierRate: &shippingv1.CarrierRate{
			CarrierRateId: req.GetCarrierRateId(),
			Owner:         req.GetCarrierRate().GetOwner(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainCarrierRateCalculation explains a carrier rate.
func (s *CarrierRateCalculationServer) ExplainCarrierRateCalculation(ctx context.Context, req *shippingv1.ExplainCarrierRateCalculationRequest) (*shippingv1.ExplainCarrierRateCalculationResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCarrierRate(ctx, req.GetContext().GetTenantId(), req.GetCarrierRate()); err != nil {
		return nil, grpcx.Wrap(err, "save carrier rate")
	}
	s.log.InfoContext(ctx, "explain carrier rate calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCarrierRate().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetCarrierRate()).(*shippingv1.CarrierRate)
	listUsersRequest := &identityv1.ListUsersRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ListUsersRequest_Filter{
			CreatedAfter: cloned.GetAudit().GetUpdatedAt(),
			Labels:       req.GetCarrierRate().GetParameters(),
		},
	}
	send := s.listUsersSender(listUsersRequest)
	listUsersReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/ListUsers")
	}
	return &shippingv1.ExplainCarrierRateCalculationResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: listUsersReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		CarrierRate: &shippingv1.CarrierRate{
			CarrierRateId: req.GetCarrierRateId(),
			Effective:     req.GetCarrierRate().GetEffective(),
		},
	}, nil
}

// GetCarrierRateRates returns a carrier rate.
func (s *CarrierRateCalculationServer) GetCarrierRateRates(ctx context.Context, req *shippingv1.GetCarrierRateRatesRequest) (*shippingv1.GetCarrierRateRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetCarrierRateRates", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "carrier_rate", req.GetCarrierRateId(), "get_carrier_rate_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get carrier rate rates", "tenant", req.GetContext().GetTenantId())
	return &shippingv1.GetCarrierRateRatesResponse{
		CarrierRate: &shippingv1.CarrierRate{CarrierRateId: req.GetCarrierRateId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// listUsersSender returns a function that sends one request to acme.identity.v1.UserService/ListUsers.
func (s *CarrierRateCalculationServer) listUsersSender(request *identityv1.ListUsersRequest) func(context.Context) (*identityv1.ListUsersResponse, error) {
	return func(ctx context.Context) (*identityv1.ListUsersResponse, error) {
		return s.userClient.ListUsers(ctx, request)
	}
}
