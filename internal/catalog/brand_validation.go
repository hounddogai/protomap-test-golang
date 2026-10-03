package catalog

import (
	"context"
	"log/slog"

	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	telematicsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/telematics/v1"
	yardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/yards/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// BrandValidationServer implements `acme.catalog.v1.BrandValidationService`.
// Checks brands against the rules before anything acts on them.
type BrandValidationServer struct {
	catalogv1.UnimplementedBrandValidationServiceServer
	store                              *Store
	log                                *slog.Logger
	categoryCalculationClient          catalogv1.CategoryCalculationServiceClient
	metricSeriesQueryClient            metricsv1.MetricSeriesQueryServiceClient
	meteredTelemetryReadingCalculation meteredTelemetryReadingCalculation
	attributeCalculationClient         catalogv1.AttributeCalculationServiceClient
	conn                               *grpc.ClientConn
	yardCalculationClient              yardsv1.YardCalculationServiceClient
	telemetryReadingCalculationClient  telematicsv1.TelemetryReadingCalculationServiceClient
	productValidationClient            catalogv1.ProductValidationServiceClient
	bus                                *eventbus.Bus
	paymentIntentValidationClient      checkoutv1.PaymentIntentValidationServiceClient
	explainProductVariantDecisionFn    func(context.Context, *catalogv1.ExplainProductVariantDecisionRequest, ...grpc.CallOption) (*catalogv1.ExplainProductVariantDecisionResponse, error)
	validateProductVariantFn           func(context.Context, *catalogv1.ValidateProductVariantRequest, ...grpc.CallOption) (*catalogv1.ValidateProductVariantResponse, error)
	productVariantCalculationClient    catalogv1.ProductVariantCalculationServiceClient
	synonymValidationClient            searchv1.SynonymValidationServiceClient
	meteredProductVariantValidation    meteredProductVariantValidation
}

// NewBrandValidationServer wires a BrandValidationServer.
func NewBrandValidationServer(deps platform.Deps) *BrandValidationServer {
	productVariantValidation := catalogv1.NewProductVariantValidationServiceClient(deps.Conn)
	return &BrandValidationServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.catalog.v1.BrandValidationService"),
		categoryCalculationClient:          catalogv1.NewCategoryCalculationServiceClient(deps.Conn),
		metricSeriesQueryClient:            metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn),
		meteredTelemetryReadingCalculation: meteredTelemetryReadingCalculation{TelemetryReadingCalculationServiceClient: telematicsv1.NewTelemetryReadingCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		attributeCalculationClient:         catalogv1.NewAttributeCalculationServiceClient(deps.Conn),
		conn:                               deps.Conn,
		yardCalculationClient:              yardsv1.NewYardCalculationServiceClient(deps.Conn),
		telemetryReadingCalculationClient:  telematicsv1.NewTelemetryReadingCalculationServiceClient(deps.Conn),
		productValidationClient:            catalogv1.NewProductValidationServiceClient(deps.Conn),
		bus:                                deps.Bus,
		paymentIntentValidationClient:      checkoutv1.NewPaymentIntentValidationServiceClient(deps.Conn),
		explainProductVariantDecisionFn:    productVariantValidation.ExplainProductVariantDecision,
		validateProductVariantFn:           productVariantValidation.ValidateProductVariant,
		productVariantCalculationClient:    catalogv1.NewProductVariantCalculationServiceClient(deps.Conn),
		synonymValidationClient:            searchv1.NewSynonymValidationServiceClient(deps.Conn),
		meteredProductVariantValidation:    meteredProductVariantValidation{ProductVariantValidationServiceClient: catalogv1.NewProductVariantValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}
}

// ValidateBrand validates a brand.
func (s *BrandValidationServer) ValidateBrand(ctx context.Context, req *catalogv1.ValidateBrandRequest) (*catalogv1.ValidateBrandResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveBrand(ctx, req.GetContext().GetTenantId(), req.GetBrand()); err != nil {
		return nil, grpcx.Wrap(err, "save brand")
	}
	s.log.InfoContext(ctx, "validate brand", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBrand().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetBrand()).(*catalogv1.Brand)
	explainCategoryCalculationRequest := &catalogv1.ExplainCategoryCalculationRequest{}
	explainCategoryCalculationRequest.Context = req.GetContext()
	explainCategoryCalculationRequest.Category = &catalogv1.Category{
		Children: []*catalogv1.Category{&catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Manager:  &identityv1.UserRef{Email: req.GetBrand().GetContact().GetEmail()},
				Audit:    &commonv1.AuditInfo{UpdatedBy: req.GetBrand().GetAudit().GetUpdatedBy()},
				Location: &commonv1.PostalAddress{Region: cloned.GetPrimaryAddress().GetRegion()},
			}},
		}},
	}
	explainCategoryCalculationReply, err := s.categoryCalculationClient.ExplainCategoryCalculation(ctx, explainCategoryCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryCalculationService/ExplainCategoryCalculation")
	}
	browseMetricSeriesesRequest := &metricsv1.BrowseMetricSeriesesRequest{}
	browseMetricSeriesesRequest.Context = req.GetContext()
	browseMetricSeriesesRequest.Filter = &metricsv1.BrowseMetricSeriesesRequest_Filter{
		Region:       req.GetBrand().GetPrimaryAddress().GetRegion(),
		CreatedAfter: req.GetBrand().GetAudit().GetUpdatedAt(),
	}
	browseMetricSeriesesReply, err := s.metricSeriesQueryClient.BrowseMetricSerieses(ctx, browseMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/BrowseMetricSerieses")
	}
	recalculateTelemetryReadingsRequest := &telematicsv1.RecalculateTelemetryReadingsRequest{
		Context: req.GetContext(),
		Details: &telematicsv1.RecalculateTelemetryReadingsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region:    req.GetBrand().GetPrimaryAddress().GetRegion(),
				Recipient: explainCategoryCalculationReply.GetCategory().GetLocation().GetRecipient(),
				Lines:     explainCategoryCalculationReply.GetCategory().GetLocation().GetLines(),
			},
		},
	}
	recalculateTelemetryReadingsReply, err := s.meteredTelemetryReadingCalculation.RecalculateTelemetryReadings(ctx, recalculateTelemetryReadingsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.telematics.v1.TelemetryReadingCalculationService/RecalculateTelemetryReadings")
	}
	estimateAttributeRequest := &catalogv1.EstimateAttributeRequest{
		Context: req.GetContext(),
		Details: &catalogv1.EstimateAttributeRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality: explainCategoryCalculationReply.GetCategory().GetLocation().GetLocality(),
			},
		},
	}
	var estimateAttributeReply *catalogv1.EstimateAttributeResponse
	if err := pipeline.Run(ctx, "acme.catalog.v1.AttributeCalculationService/EstimateAttribute", func(ctx context.Context) error {
		answer, err := s.attributeCalculationClient.EstimateAttribute(ctx, estimateAttributeRequest)
		estimateAttributeReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.AttributeCalculationService/EstimateAttribute")
	}
	_ = estimateAttributeReply
	return &catalogv1.ValidateBrandResponse{
		Brand: &catalogv1.Brand{
			RelationshipManager: &identityv1.UserRef{
				UserId: explainCategoryCalculationReply.GetCategory().GetManager().GetUserId(),
			},
			BrandId: req.GetBrandId(),
			Contact: req.GetDetails().GetContact(),
		},
		Result: &commonv1.OperationResult{
			Status:  explainCategoryCalculationReply.GetResult().GetStatus(),
			Message: browseMetricSeriesesReply.GetResult().GetMessage(),
			Details: recalculateTelemetryReadingsReply.GetResult().GetDetails(),
		},
		Outcome: &catalogv1.ValidateBrandResponse_Outcome{Message: browseMetricSeriesesReply.GetResult().GetMessage()},
	}, nil
}

// PreviewBrand previews a brand.
func (s *BrandValidationServer) PreviewBrand(ctx context.Context, req *catalogv1.PreviewBrandRequest) (*catalogv1.PreviewBrandResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveBrand(ctx, req.GetContext().GetTenantId(), req.GetBrand()); err != nil {
		return nil, grpcx.Wrap(err, "save brand")
	}
	s.log.InfoContext(ctx, "preview brand", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBrand().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetBrand())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	sendSearchQueryTestNoticeRequest := &searchv1.SendSearchQueryTestNoticeRequest{
		Context: req.GetContext(),
		Details: &searchv1.SendSearchQueryTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
			Payload:   packed,
		},
	}
	sendSearchQueryTestNoticeReply, err := grpcx.NewClient(s.conn, searchv1.NewSearchQueryNotificationServiceClient).SendSearchQueryTestNotice(ctx, sendSearchQueryTestNoticeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/SendSearchQueryTestNotice")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := brandValidationServerEstimateYardSteps[mode]
	ctx = tenancy.With(ctx, "recipient", req.GetBrand().GetPrimaryAddress().GetRecipient())
	cloned := proto.Clone(req.GetBrand()).(*catalogv1.Brand)
	estimateYardRequest := &yardsv1.EstimateYardRequest{
		Context: req.GetContext(),
		Details: &yardsv1.EstimateYardRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region:    req.GetBrand().GetPrimaryAddress().GetRegion(),
				Recipient: tenancy.Value(ctx, "recipient"),
			},
		},
		Yard: &yardsv1.Yard{
			Locator: &yardsv1.Yard_Address{Address: &commonv1.PostalAddress{Locality: cloned.GetPrimaryAddress().GetLocality()}},
			Audit:   &commonv1.AuditInfo{CreatedBy: req.GetBrand().GetAudit().GetCreatedBy()},
		},
	}
	estimateYardReply, err := run(s, ctx, estimateYardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.yards.v1.YardCalculationService/EstimateYard")
	}
	cloned2 := proto.Clone(req.GetDetails()).(*catalogv1.PreviewBrandRequest_Details)
	explainTelemetryReadingCalculationRequest := &telematicsv1.ExplainTelemetryReadingCalculationRequest{}
	explainTelemetryReadingCalculationRequest.Context = req.GetContext()
	explainTelemetryReadingCalculationRequest.Details = &telematicsv1.ExplainTelemetryReadingCalculationRequest_Details{
		Contact: &commonv1.ContactInfo{
			Phone:            cloned2.GetContact().GetPhone(),
			PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			Email:            estimateYardReply.GetYard().GetOwner().GetEmail(),
		},
	}
	explainTelemetryReadingCalculationReply, err := s.telemetryReadingCalculationClient.ExplainTelemetryReadingCalculation(ctx, explainTelemetryReadingCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.telematics.v1.TelemetryReadingCalculationService/ExplainTelemetryReadingCalculation")
	}
	return &catalogv1.PreviewBrandResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: sendSearchQueryTestNoticeReply.GetSearchQuery().GetCorrelationId(),
			Message:       explainTelemetryReadingCalculationReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		Brand: &catalogv1.Brand{
			PrimaryAddress: &commonv1.PostalAddress{Lines: estimateYardReply.GetYard().GetAddress().GetLines()},
			Audit: &commonv1.AuditInfo{
				CreatedAt: estimateYardReply.GetYard().GetAudit().GetCreatedAt(),
			},
			BrandId: req.GetBrandId(),
			Contact: &commonv1.ContactInfo{Name: req.GetBrand().GetName()},
		},
		Outcome: &catalogv1.PreviewBrandResponse_Outcome{
			Status: explainTelemetryReadingCalculationReply.GetResult().GetStatus(),
		},
	}, nil
}

// CheckBrandEligibility checks a brand.
func (s *BrandValidationServer) CheckBrandEligibility(ctx context.Context, req *catalogv1.CheckBrandEligibilityRequest) (*catalogv1.CheckBrandEligibilityResponse, error) {
	if err := s.store.SaveBrand(ctx, req.GetContext().GetTenantId(), req.GetBrand()); err != nil {
		return nil, grpcx.Wrap(err, "save brand")
	}
	s.log.InfoContext(ctx, "check brand eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBrand().GetAudit().GetEtag())
	countMetricSeriesesRequest := &metricsv1.CountMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter: &metricsv1.CountMetricSeriesesRequest_Filter{
			Region:       req.GetBrand().GetPrimaryAddress().GetRegion(),
			CreatedAfter: req.GetBrand().GetPassport().GetExpiresAt(),
			OwnerId:      req.GetBrand().GetRelationshipManager().GetUserId(),
		},
	}
	send := s.countMetricSeriesesSender(countMetricSeriesesRequest)
	countMetricSeriesesReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/CountMetricSerieses")
	}
	return &catalogv1.CheckBrandEligibilityResponse{
		Outcome: &catalogv1.CheckBrandEligibilityResponse_Outcome{Message: countMetricSeriesesReply.GetResult().GetMessage()},
		Result: &commonv1.OperationResult{
			CorrelationId: countMetricSeriesesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Brand: &catalogv1.Brand{
			BrandId: req.GetBrandId(),
			Status:  req.GetBrand().GetStatus(),
		},
	}, nil
}

// NormalizeBrand normalizes a brand.
func (s *BrandValidationServer) NormalizeBrand(ctx context.Context, req *catalogv1.NormalizeBrandRequest) (*catalogv1.NormalizeBrandResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveBrand(ctx, req.GetContext().GetTenantId(), req.GetBrand()); err != nil {
		return nil, grpcx.Wrap(err, "save brand")
	}
	s.log.InfoContext(ctx, "normalize brand", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBrand().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetBrand())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded catalogv1.Brand
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	recalculateProductVariantsRequest := &catalogv1.RecalculateProductVariantsRequest{
		Context: req.GetContext(),
		ProductVariant: &catalogv1.ProductVariant{
			Locator: &catalogv1.ProductVariant_Address{Address: &commonv1.PostalAddress{
				CountryCode: (&decoded).GetPrimaryAddress().GetCountryCode(),
			}},
		},
		Details: &catalogv1.RecalculateProductVariantsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: req.GetBrand().GetPrimaryAddress().GetRecipient(),
			},
		},
	}
	recalculateProductVariantsReply, err := grpcx.NewClient(s.conn, catalogv1.NewProductVariantCalculationServiceClient).RecalculateProductVariants(ctx, recalculateProductVariantsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantCalculationService/RecalculateProductVariants")
	}
	explainProductDecisionRequest := &catalogv1.ExplainProductDecisionRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Dimensions: &catalogv1.ProductDimensions{
				Height: recalculateProductVariantsReply.GetProductVariant().GetDimensions().GetHeight(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: recalculateProductVariantsReply.GetProductVariant().GetAudit().GetUpdatedBy(),
			},
		},
	}
	explainProductDecisionReply, err := grpcx.Call(ctx, s.productValidationClient.ExplainProductDecision, explainProductDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductValidationService/ExplainProductDecision")
	}
	return &catalogv1.NormalizeBrandResponse{
		Outcome: &catalogv1.NormalizeBrandResponse_Outcome{
			Message: recalculateProductVariantsReply.GetResult().GetMessage(),
		},
		Brand: &catalogv1.Brand{
			PrimaryAddress: &commonv1.PostalAddress{
				Region: explainProductDecisionReply.GetProduct().GetAddress().GetRegion(),
			},
			BrandId: req.GetBrandId(),
			Name:    req.GetBrand().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainBrandDecision explains a brand.
func (s *BrandValidationServer) ExplainBrandDecision(ctx context.Context, req *catalogv1.ExplainBrandDecisionRequest) (*catalogv1.ExplainBrandDecisionResponse, error) {
	if err := s.store.SaveBrand(ctx, req.GetContext().GetTenantId(), req.GetBrand()); err != nil {
		return nil, grpcx.Wrap(err, "save brand")
	}
	s.log.InfoContext(ctx, "explain brand decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBrand().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "catalog.explain_brand_decision.normalize_order_amendment",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"order_amendment.audit.etag":               req.GetBrand().GetAudit().GetEtag(),
			"order_amendment.billing_address.locality": req.GetBrand().GetPrimaryAddress().GetLocality(),
			"order_amendment.audit.updated_by":         req.GetBrand().GetAudit().GetUpdatedBy(),
			"details.contact.email":                    req.GetBrand().GetContact().GetEmail(),
			"context.tenant_id":                        req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	previewPaymentIntentRequest := &checkoutv1.PreviewPaymentIntentRequest{
		Context: req.GetContext(),
		PaymentIntent: &checkoutv1.PaymentIntent{
			Requester: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
		},
	}
	var previewPaymentIntentReply *checkoutv1.PreviewPaymentIntentResponse
	if err := pipeline.Run(ctx, "acme.checkout.v1.PaymentIntentValidationService/PreviewPaymentIntent", func(ctx context.Context) error {
		answer, err := s.paymentIntentValidationClient.PreviewPaymentIntent(ctx, previewPaymentIntentRequest)
		previewPaymentIntentReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.PaymentIntentValidationService/PreviewPaymentIntent")
	}
	explainProductVariantDecisionRequest := &catalogv1.ExplainProductVariantDecisionRequest{
		Context: req.GetContext(),
		ProductVariant: &catalogv1.ProductVariant{
			Owner: &identityv1.UserRef{
				UserId: previewPaymentIntentReply.GetPaymentIntent().GetAssignee().GetUserId(),
			},
		},
	}
	explainProductVariantDecisionReply, err := s.explainProductVariantDecisionFn(ctx, explainProductVariantDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantValidationService/ExplainProductVariantDecision")
	}
	return &catalogv1.ExplainBrandDecisionResponse{
		Result: &commonv1.OperationResult{
			Message: previewPaymentIntentReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Brand: &catalogv1.Brand{
			RelationshipManager: &identityv1.UserRef{
				Email: previewPaymentIntentReply.GetPaymentIntent().GetAssignee().GetEmail(),
			},
			PrimaryAddress: &commonv1.PostalAddress{
				Region: explainProductVariantDecisionReply.GetProductVariant().GetAddress().GetRegion(),
			},
			BrandId: req.GetBrandId(),
			Contact: req.GetBrand().GetContact(),
		},
	}, nil
}

// ScoreBrand scores a brand.
func (s *BrandValidationServer) ScoreBrand(ctx context.Context, req *catalogv1.ScoreBrandRequest) (*catalogv1.ScoreBrandResponse, error) {
	if err := s.store.SaveBrand(ctx, req.GetContext().GetTenantId(), req.GetBrand()); err != nil {
		return nil, grpcx.Wrap(err, "save brand")
	}
	s.log.InfoContext(ctx, "score brand", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBrand().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetBrand()).(*catalogv1.Brand)
	validateProductVariantRequest := &catalogv1.ValidateProductVariantRequest{
		Context: req.GetContext(),
		ProductVariant: &catalogv1.ProductVariant{
			Locator: &catalogv1.ProductVariant_Address{Address: &commonv1.PostalAddress{Lines: req.GetBrand().GetPrimaryAddress().GetLines()}},
			Owner: &identityv1.UserRef{
				DisplayName: cloned.GetRelationshipManager().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetBrand().GetAudit().GetUpdatedBy()},
		},
		Details: &catalogv1.ValidateProductVariantRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetBrand().GetContact().GetName()},
		},
	}
	validateProductVariantReply, err := s.validateProductVariantFn(ctx, validateProductVariantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantValidationService/ValidateProductVariant")
	}
	return &catalogv1.ScoreBrandResponse{
		Brand: &catalogv1.Brand{
			Audit: &commonv1.AuditInfo{
				CreatedBy: validateProductVariantReply.GetProductVariant().GetAudit().GetCreatedBy(),
			},
			PrimaryAddress: &commonv1.PostalAddress{
				CountryCode: validateProductVariantReply.GetProductVariant().GetAddress().GetCountryCode(),
			},
			BrandId: req.GetBrandId(),
			Contact: &commonv1.ContactInfo{Name: req.GetBrand().GetName()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LintBrands lints a brand.
func (s *BrandValidationServer) LintBrands(ctx context.Context, req *catalogv1.LintBrandsRequest) (*catalogv1.LintBrandsResponse, error) {
	if err := s.store.SaveBrand(ctx, req.GetContext().GetTenantId(), req.GetBrand()); err != nil {
		return nil, grpcx.Wrap(err, "save brand")
	}
	s.log.InfoContext(ctx, "lint brands", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBrand().GetAudit().GetEtag())
	recalculateProductVariantsReply, err := s.productVariantCalculationClient.RecalculateProductVariants(ctx, &catalogv1.RecalculateProductVariantsRequest{
		Context: req.GetContext(),
		ProductVariant: &catalogv1.ProductVariant{
			Locator: &catalogv1.ProductVariant_Address{Address: &commonv1.PostalAddress{Lines: req.GetBrand().GetPrimaryAddress().GetLines()}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantCalculationService/RecalculateProductVariants")
	}
	explainSynonymDecisionRequest := &searchv1.ExplainSynonymDecisionRequest{}
	explainSynonymDecisionRequest.Context = req.GetContext()
	explainSynonymDecisionRequest.Details = &searchv1.ExplainSynonymDecisionRequest_Details{
		Contact: &commonv1.ContactInfo{
			Name:  req.GetBrand().GetContact().GetName(),
			Email: req.GetDetails().GetContact().GetEmail(),
		},
	}
	explainSynonymDecisionRequest.Synonym = &searchv1.Synonym{
		Audit: &commonv1.AuditInfo{
			CreatedAt: recalculateProductVariantsReply.GetProductVariant().GetAudit().GetCreatedAt(),
		},
	}
	explainSynonymDecisionReply, err := s.synonymValidationClient.ExplainSynonymDecision(ctx, explainSynonymDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/ExplainSynonymDecision")
	}
	return &catalogv1.LintBrandsResponse{
		Brand: &catalogv1.Brand{
			PrimaryAddress: &commonv1.PostalAddress{
				Lines: recalculateProductVariantsReply.GetProductVariant().GetAddress().GetLines(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: recalculateProductVariantsReply.GetProductVariant().GetAudit().GetUpdatedAt(),
			},
			BrandId: req.GetBrandId(),
			Status:  req.GetBrand().GetStatus(),
		},
		Outcome: &catalogv1.LintBrandsResponse_Outcome{
			Message: explainSynonymDecisionReply.GetOutcome().GetMessage(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetBrandRules returns a brand.
func (s *BrandValidationServer) GetBrandRules(ctx context.Context, req *catalogv1.GetBrandRulesRequest) (*catalogv1.GetBrandRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetBrandRules", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "brand", req.GetBrandId(), "get_brand_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get brand rules", "tenant", req.GetContext().GetTenantId())
	previewProductVariantRequest := &catalogv1.PreviewProductVariantRequest{
		Context: req.GetContext(),
		Details: &catalogv1.PreviewProductVariantRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		ProductVariant: &catalogv1.ProductVariant{
			Owner:      &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Components: map[string]*catalogv1.ProductVariantComponent{"primary": &catalogv1.ProductVariantComponent{ComponentId: req.GetContext().GetTraceId()}},
		},
	}
	previewProductVariantReply, err := s.meteredProductVariantValidation.PreviewProductVariant(ctx, previewProductVariantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantValidationService/PreviewProductVariant")
	}
	return &catalogv1.GetBrandRulesResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: previewProductVariantReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Brand: &catalogv1.Brand{BrandId: req.GetBrandId()},
	}, nil
}

// brandValidationServerEstimateYardSteps are the ways to run acme.yards.v1.YardCalculationService/EstimateYard, by
// name.
var brandValidationServerEstimateYardSteps = map[string]func(*BrandValidationServer, context.Context, *yardsv1.EstimateYardRequest) (*yardsv1.EstimateYardResponse, error){
	"default": (*BrandValidationServer).estimateYardStep,
	"audited": (*BrandValidationServer).estimateYardAuditedStep,
}

func (s *BrandValidationServer) estimateYardStep(ctx context.Context, request *yardsv1.EstimateYardRequest) (*yardsv1.EstimateYardResponse, error) {
	return s.yardCalculationClient.EstimateYard(ctx, request)
}

func (s *BrandValidationServer) estimateYardAuditedStep(ctx context.Context, request *yardsv1.EstimateYardRequest) (*yardsv1.EstimateYardResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.yards.v1.YardCalculationService/EstimateYard")
	return s.yardCalculationClient.EstimateYard(ctx, request)
}

// countMetricSeriesesSender returns a function that sends one request to
// acme.metrics.v1.MetricSeriesQueryService/CountMetricSerieses.
func (s *BrandValidationServer) countMetricSeriesesSender(request *metricsv1.CountMetricSeriesesRequest) func(context.Context) (*metricsv1.CountMetricSeriesesResponse, error) {
	return func(ctx context.Context) (*metricsv1.CountMetricSeriesesResponse, error) {
		return s.metricSeriesQueryClient.CountMetricSerieses(ctx, request)
	}
}
