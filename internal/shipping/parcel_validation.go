package shipping

import (
	"context"
	"fmt"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// ParcelValidationServer implements `acme.shipping.v1.ParcelValidationService`.
// Checks parcels against the rules before anything acts on them.
type ParcelValidationServer struct {
	shippingv1.UnimplementedParcelValidationServiceServer
	store                           *Store
	log                             *slog.Logger
	carrierRateCalculationClient    shippingv1.CarrierRateCalculationServiceClient
	lintShipmentsFn                 func(context.Context, *shippingv1.LintShipmentsRequest, ...grpc.CallOption) (*shippingv1.LintShipmentsResponse, error)
	parcelCalculationClient         shippingv1.ParcelCalculationServiceClient
	returnRequestValidationClient   returnsv1.ReturnRequestValidationServiceClient
	featureFlagValidationClient     featureflagsv1.FeatureFlagValidationServiceClient
	conn                            *grpc.ClientConn
	accessPolicyQueryClient         authorizationv1.AccessPolicyQueryServiceClient
	scoreFeatureFlagFn              func(context.Context, *featureflagsv1.ScoreFeatureFlagRequest, ...grpc.CallOption) (*featureflagsv1.ScoreFeatureFlagResponse, error)
	shipmentCalculationClient       shippingv1.ShipmentCalculationServiceClient
	parcelCalculationClientFallback shippingv1.ParcelCalculationServiceClient
	listMetricSeriesesFn            func(context.Context, *metricsv1.ListMetricSeriesesRequest, ...grpc.CallOption) (*metricsv1.ListMetricSeriesesResponse, error)
}

// NewParcelValidationServer wires a ParcelValidationServer.
func NewParcelValidationServer(deps platform.Deps) *ParcelValidationServer {
	shipmentValidation := shippingv1.NewShipmentValidationServiceClient(deps.Conn)
	featureFlagValidation := featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn)
	metricSeries := metricsv1.NewMetricSeriesServiceClient(deps.Conn)
	return &ParcelValidationServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.shipping.v1.ParcelValidationService"),
		carrierRateCalculationClient:    shippingv1.NewCarrierRateCalculationServiceClient(deps.Conn),
		lintShipmentsFn:                 shipmentValidation.LintShipments,
		parcelCalculationClient:         shippingv1.NewParcelCalculationServiceClient(deps.Conn),
		returnRequestValidationClient:   returnsv1.NewReturnRequestValidationServiceClient(deps.Conn),
		featureFlagValidationClient:     featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn),
		conn:                            deps.Conn,
		accessPolicyQueryClient:         authorizationv1.NewAccessPolicyQueryServiceClient(deps.Conn),
		scoreFeatureFlagFn:              featureFlagValidation.ScoreFeatureFlag,
		shipmentCalculationClient:       shippingv1.NewShipmentCalculationServiceClient(deps.Conn),
		parcelCalculationClientFallback: shippingv1.NewParcelCalculationServiceClient(deps.Conn),
		listMetricSeriesesFn:            metricSeries.ListMetricSerieses,
	}
}

// ValidateParcel validates a parcel.
func (s *ParcelValidationServer) ValidateParcel(ctx context.Context, req *shippingv1.ValidateParcelRequest) (*shippingv1.ValidateParcelResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "validate parcel", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	detailsCache := cache.New[string, *shippingv1.ValidateParcelRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	explainCarrierRateCalculationReply, err := s.carrierRateCalculationClient.ExplainCarrierRateCalculation(ctx, &shippingv1.ExplainCarrierRateCalculationRequest{
		Context: req.GetContext(),
		CarrierRate: &shippingv1.CarrierRate{
			Owner: &identityv1.UserRef{Email: req.GetParcel().GetOwner().GetEmail()},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetParcel().GetAudit().GetCreatedAt()},
		},
		Details: &shippingv1.ExplainCarrierRateCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Email: cachedDetails.GetContact().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateCalculationService/ExplainCarrierRateCalculation")
	}
	var holder struct{ Etag string }
	if record := req.GetParcel().GetAudit(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Etag"); err != nil {
			s.log.WarnContext(ctx, "copy etag", "error", err)
		}
	}
	calculateCarrierRateRequest := &shippingv1.CalculateCarrierRateRequest{
		Context:     req.GetContext(),
		CarrierRate: &shippingv1.CarrierRate{Audit: &commonv1.AuditInfo{Etag: holder.Etag}},
	}
	calculateCarrierRateReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*shippingv1.CalculateCarrierRateResponse, error) {
		return s.carrierRateCalculationClient.CalculateCarrierRate(ctx, calculateCarrierRateRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateCalculationService/CalculateCarrierRate")
	}
	lintShipmentsRequest := &shippingv1.LintShipmentsRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			Lines: []*shippingv1.ShipmentLine{&shippingv1.ShipmentLine{
				Tax: &shippingv1.ShipmentLineTax{
					Amount: calculateCarrierRateReply.GetCarrierRate().GetDiscount().GetAmount(),
				},
			}},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetParcel().GetAudit().GetUpdatedAt()},
		},
	}
	lintShipmentsReply, err := s.lintShipmentsFn(ctx, lintShipmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentValidationService/LintShipments")
	}
	return &shippingv1.ValidateParcelResponse{
		Result: &commonv1.OperationResult{
			Status:  explainCarrierRateCalculationReply.GetResult().GetStatus(),
			Details: calculateCarrierRateReply.GetResult().GetDetails(),
		},
		Parcel: &shippingv1.Parcel{
			Owner: &identityv1.UserRef{
				DisplayName: explainCarrierRateCalculationReply.GetCarrierRate().GetOwner().GetDisplayName(),
			},
			Components: map[string]*shippingv1.ParcelComponent{"primary": &shippingv1.ParcelComponent{
				Description: calculateCarrierRateReply.GetCarrierRate().GetDescription(),
			}},
			Code: lintShipmentsReply.GetShipment().GetCounterparty().GetCode(),
			Audit: &commonv1.AuditInfo{
				CreatedBy: lintShipmentsReply.GetShipment().GetAudit().GetCreatedBy(),
			},
			ParcelId: req.GetParcelId(),
			Capacity: req.GetParcel().GetCapacity(),
		},
	}, nil
}

// PreviewParcel previews a parcel.
func (s *ParcelValidationServer) PreviewParcel(ctx context.Context, req *shippingv1.PreviewParcelRequest) (*shippingv1.PreviewParcelResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PreviewParcel", "error", err)
		return nil, err
	}
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "preview parcel", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	estimateParcelRequest := buildEstimateParcelRequestForPreviewParcel(req)
	estimateParcelReply, err := s.parcelCalculationClient.EstimateParcel(ctx, estimateParcelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/EstimateParcel")
	}
	compareParcelOptionsReply, err := s.parcelCalculationClient.CompareParcelOptions(ctx, &shippingv1.CompareParcelOptionsRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Dimensions: &shippingv1.ParcelDimensions{
				Length: estimateParcelReply.GetParcel().GetDimensions().GetLength(),
			},
		},
		Details: &shippingv1.CompareParcelOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{Recipient: req.GetParcel().GetAddress().GetRecipient()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/CompareParcelOptions")
	}
	return &shippingv1.PreviewParcelResponse{
		Parcel: &shippingv1.Parcel{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: estimateParcelReply.GetParcel().GetAudit().GetUpdatedAt(),
			},
			Components: map[string]*shippingv1.ParcelComponent{"primary": &shippingv1.ParcelComponent{
				Quantity: compareParcelOptionsReply.GetParcel().GetComponents()["primary"].GetQuantity(),
			}},
			ParcelId: req.GetParcelId(),
			Status:   req.GetParcel().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckParcelEligibility checks a parcel.
func (s *ParcelValidationServer) CheckParcelEligibility(ctx context.Context, req *shippingv1.CheckParcelEligibilityRequest) (*shippingv1.CheckParcelEligibilityResponse, error) {
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "check parcel eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	var countryCode string
	switch choice := req.GetParcel().GetLocator().(type) {
	case *shippingv1.Parcel_Address:
		countryCode = choice.Address.GetCountryCode()
	case *shippingv1.Parcel_FacilityCode:
		countryCode = choice.FacilityCode
	}
	explainReturnRequestDecisionRequest := &returnsv1.ExplainReturnRequestDecisionRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: countryCode,
				Region:      req.GetParcel().GetAddress().GetRegion(),
			},
		},
	}
	explainReturnRequestDecisionReply, err := grpcx.Call(ctx, s.returnRequestValidationClient.ExplainReturnRequestDecision, explainReturnRequestDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestValidationService/ExplainReturnRequestDecision")
	}
	normalizeFeatureFlagRequest := &featureflagsv1.NormalizeFeatureFlagRequest{}
	normalizeFeatureFlagRequest.Context = req.GetContext()
	normalizeFeatureFlagRequest.FeatureFlag = &featureflagsv1.FeatureFlag{
		Owner: &identityv1.UserRef{
			DisplayName: req.GetParcel().GetOwner().GetDisplayName(),
			Email:       explainReturnRequestDecisionReply.GetReturnRequest().GetCounterparty().GetEmail(),
		},
		Audit: &commonv1.AuditInfo{
			CreatedBy: explainReturnRequestDecisionReply.GetReturnRequest().GetAudit().GetCreatedBy(),
		},
	}
	normalizeFeatureFlagReply, err := s.featureFlagValidationClient.NormalizeFeatureFlag(ctx, normalizeFeatureFlagRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/NormalizeFeatureFlag")
	}
	var postalCode string
	switch choice2 := req.GetParcel().GetLocator().(type) {
	case *shippingv1.Parcel_Address:
		postalCode = choice2.Address.GetPostalCode()
	case *shippingv1.Parcel_FacilityCode:
		postalCode = choice2.FacilityCode
	}
	calculateParcelRequest := &shippingv1.CalculateParcelRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Dimensions: &shippingv1.ParcelDimensions{Length: req.GetParcel().GetDimensions().GetLength()},
			Owner: &identityv1.UserRef{
				DisplayName: req.GetParcel().GetOwner().GetDisplayName(),
				UserId:      normalizeFeatureFlagReply.GetFeatureFlag().GetOwner().GetUserId(),
			},
			Locator: &shippingv1.Parcel_Address{Address: &commonv1.PostalAddress{PostalCode: postalCode}},
		},
	}
	type calculateParcelResult struct {
		reply *shippingv1.CalculateParcelResponse
		err   error
	}
	calculateParcelResults := make(chan calculateParcelResult, 1)
	go func() {
		reply, err := s.parcelCalculationClient.CalculateParcel(ctx, calculateParcelRequest)
		calculateParcelResults <- calculateParcelResult{reply, err}
	}()
	var calculateParcelReply *shippingv1.CalculateParcelResponse
	select {
	case result := <-calculateParcelResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.shipping.v1.ParcelCalculationService/CalculateParcel")
		}
		calculateParcelReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	compareParcelOptionsRequest := buildCompareParcelOptionsRequestForCheckParcelEligibility(req, explainReturnRequestDecisionReply, calculateParcelReply)
	compareParcelOptionsReply, err := s.parcelCalculationClient.CompareParcelOptions(ctx, compareParcelOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/CompareParcelOptions")
	}
	return &shippingv1.CheckParcelEligibilityResponse{
		Result: &commonv1.OperationResult{
			Details:      explainReturnRequestDecisionReply.GetResult().GetDetails(),
			ReferenceIds: normalizeFeatureFlagReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Parcel: &shippingv1.Parcel{
			Components: map[string]*shippingv1.ParcelComponent{"primary": &shippingv1.ParcelComponent{
				ComponentId: calculateParcelReply.GetParcel().GetComponents()["primary"].GetComponentId(),
			}},
			Capacity: compareParcelOptionsReply.GetParcel().GetCapacity(),
			ParcelId: req.GetParcelId(),
			Name:     req.GetParcel().GetName(),
		},
	}, nil
}

// NormalizeParcel normalizes a parcel.
func (s *ParcelValidationServer) NormalizeParcel(ctx context.Context, req *shippingv1.NormalizeParcelRequest) (*shippingv1.NormalizeParcelResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "normalize parcel", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	var countryCode string
	switch choice := req.GetParcel().GetLocator().(type) {
	case *shippingv1.Parcel_Address:
		countryCode = choice.Address.GetCountryCode()
	case *shippingv1.Parcel_FacilityCode:
		countryCode = choice.FacilityCode
	}
	encoded, err := protojson.Marshal(req.GetParcel())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded shippingv1.Parcel
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	estimateParcelRequest := &shippingv1.EstimateParcelRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Locator: &shippingv1.Parcel_Address{Address: &commonv1.PostalAddress{Locality: req.GetParcel().GetAddress().GetLocality()}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetParcel().GetAudit().GetCreatedAt(),
				UpdatedAt: (&decoded).GetAudit().GetUpdatedAt(),
			},
		},
		Details: &shippingv1.EstimateParcelRequest_Details{
			Destination: &commonv1.PostalAddress{CountryCode: countryCode},
		},
	}
	estimateParcelReply := new(shippingv1.EstimateParcelResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", shippingParcelCalculationServiceName, "EstimateParcel"), estimateParcelRequest, estimateParcelReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/EstimateParcel")
	}
	return &shippingv1.NormalizeParcelResponse{
		Parcel: &shippingv1.Parcel{
			Components: map[string]*shippingv1.ParcelComponent{"primary": &shippingv1.ParcelComponent{
				Description: estimateParcelReply.GetParcel().GetComponents()["primary"].GetDescription(),
			}},
			ParcelId: req.GetParcelId(),
			Name:     req.GetParcel().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainParcelDecision explains a parcel.
func (s *ParcelValidationServer) ExplainParcelDecision(ctx context.Context, req *shippingv1.ExplainParcelDecisionRequest) (*shippingv1.ExplainParcelDecisionResponse, error) {
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "explain parcel decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetParcel()).(*shippingv1.Parcel)
	browseAccessPoliciesReply, err := s.accessPolicyQueryClient.BrowseAccessPolicies(ctx, &authorizationv1.BrowseAccessPoliciesRequest{
		Context: req.GetContext(),
		Filter: &authorizationv1.BrowseAccessPoliciesRequest_Filter{
			Region:       req.GetParcel().GetAddress().GetRegion(),
			CreatedAfter: cloned.GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetParcel().GetOwner().GetUserId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyQueryService/BrowseAccessPolicies")
	}
	var countryCode string
	switch choice := req.GetParcel().GetLocator().(type) {
	case *shippingv1.Parcel_Address:
		countryCode = choice.Address.GetCountryCode()
	case *shippingv1.Parcel_FacilityCode:
		countryCode = choice.FacilityCode
	}
	scoreFeatureFlagRequest := &featureflagsv1.ScoreFeatureFlagRequest{
		Context: req.GetContext(),
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetParcel().GetAudit().GetCreatedBy(),
				Etag:      req.GetParcel().GetAudit().GetEtag(),
			},
		},
		Details: &featureflagsv1.ScoreFeatureFlagRequest_Details{
			Destination: &commonv1.PostalAddress{CountryCode: countryCode},
		},
	}
	scoreFeatureFlagReply, err := s.scoreFeatureFlagFn(ctx, scoreFeatureFlagRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/ScoreFeatureFlag")
	}
	return &shippingv1.ExplainParcelDecisionResponse{
		Result: &commonv1.OperationResult{
			Message: browseAccessPoliciesReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Parcel: &shippingv1.Parcel{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: scoreFeatureFlagReply.GetFeatureFlag().GetAudit().GetUpdatedAt(),
			},
			ParcelId: req.GetParcelId(),
			Status:   req.GetParcel().GetStatus(),
		},
	}, nil
}

// ScoreParcel scores a parcel.
func (s *ParcelValidationServer) ScoreParcel(ctx context.Context, req *shippingv1.ScoreParcelRequest) (*shippingv1.ScoreParcelResponse, error) {
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "score parcel", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	quoteShipmentRequest := &shippingv1.QuoteShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			Lines: []*shippingv1.ShipmentLine{&shippingv1.ShipmentLine{
				Description: req.GetParcel().GetComponents()["primary"].GetDescription(),
			}},
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetParcel().GetAudit().GetEtag(),
				CreatedBy: req.GetParcel().GetAudit().GetCreatedBy(),
			},
		},
		Details: &shippingv1.QuoteShipmentRequest_Details{
			Destination: &commonv1.PostalAddress{PostalCode: req.GetParcel().GetAddress().GetPostalCode()},
		},
	}
	quoteShipmentReply, err := grpcx.Call(ctx, s.shipmentCalculationClient.QuoteShipment, quoteShipmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/QuoteShipment")
	}
	return &shippingv1.ScoreParcelResponse{
		Result: &commonv1.OperationResult{
			Message: quoteShipmentReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Parcel: &shippingv1.Parcel{
			Audit:    &commonv1.AuditInfo{Etag: quoteShipmentReply.GetShipment().GetAudit().GetEtag()},
			ParcelId: req.GetParcelId(),
			Name:     req.GetParcel().GetName(),
		},
	}, nil
}

// LintParcels lints a parcel.
func (s *ParcelValidationServer) LintParcels(ctx context.Context, req *shippingv1.LintParcelsRequest) (*shippingv1.LintParcelsResponse, error) {
	if err := s.store.SaveParcel(ctx, req.GetContext().GetTenantId(), req.GetParcel()); err != nil {
		return nil, grpcx.Wrap(err, "save parcel")
	}
	s.log.InfoContext(ctx, "lint parcels", "tenant", req.GetContext().GetTenantId(), "etag", req.GetParcel().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *shippingv1.CompareParcelOptionsRequest, ...grpc.CallOption) (*shippingv1.CompareParcelOptionsResponse, error){
		"primary":  s.parcelCalculationClient.CompareParcelOptions,
		"fallback": s.parcelCalculationClientFallback.CompareParcelOptions,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	compareParcelOptionsRequest := &shippingv1.CompareParcelOptionsRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Components: map[string]*shippingv1.ParcelComponent{"primary": &shippingv1.ParcelComponent{
				Description: req.GetParcel().GetComponents()["primary"].GetDescription(),
			}},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetParcel().GetAudit().GetUpdatedAt()},
		},
		Details: &shippingv1.CompareParcelOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{PostalCode: req.GetParcel().GetAddress().GetPostalCode()},
		},
	}
	compareParcelOptionsReply, err := route(ctx, compareParcelOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/CompareParcelOptions")
	}
	listMetricSeriesesRequest := &metricsv1.ListMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter: &metricsv1.ListMetricSeriesesRequest_Filter{
			Region:       req.GetParcel().GetAddress().GetRegion(),
			CreatedAfter: compareParcelOptionsReply.GetParcel().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetParcel().GetComponents()["primary"].GetComponentId(),
			Labels:       compareParcelOptionsReply.GetResult().GetDetails(),
		},
	}
	listMetricSeriesesReply, err := s.listMetricSeriesesFn(ctx, listMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesService/ListMetricSerieses")
	}
	return &shippingv1.LintParcelsResponse{
		Parcel: &shippingv1.Parcel{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: compareParcelOptionsReply.GetParcel().GetAudit().GetUpdatedBy(),
			},
			ParcelId: req.GetParcelId(),
			Owner:    req.GetParcel().GetOwner(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: listMetricSeriesesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &shippingv1.LintParcelsResponse_Outcome{Status: listMetricSeriesesReply.GetResult().GetStatus()},
	}, nil
}

// GetParcelRules returns a parcel.
func (s *ParcelValidationServer) GetParcelRules(ctx context.Context, req *shippingv1.GetParcelRulesRequest) (*shippingv1.GetParcelRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetParcelRules", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "parcel", req.GetParcelId(), "get_parcel_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get parcel rules", "tenant", req.GetContext().GetTenantId())
	estimateSellerScorecardRequest := &sellersv1.EstimateSellerScorecardRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			Counterparty: &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	estimateSellerScorecardReply, err := grpcx.NewClient(s.conn, sellersv1.NewSellerScorecardCalculationServiceClient).EstimateSellerScorecard(ctx, estimateSellerScorecardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardCalculationService/EstimateSellerScorecard")
	}
	return &shippingv1.GetParcelRulesResponse{
		Parcel: &shippingv1.Parcel{
			Audit: &commonv1.AuditInfo{
				CreatedBy: estimateSellerScorecardReply.GetSellerScorecard().GetAudit().GetCreatedBy(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: estimateSellerScorecardReply.GetSellerScorecard().GetCounterparty().GetDisplayName(),
			},
			ParcelId: req.GetParcelId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
