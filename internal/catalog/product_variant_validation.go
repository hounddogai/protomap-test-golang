package catalog

import (
	"context"
	"log/slog"

	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// ProductVariantValidationServer implements `acme.catalog.v1.ProductVariantValidationService`.
// Checks product variants against the rules before anything acts on them.
type ProductVariantValidationServer struct {
	catalogv1.UnimplementedProductVariantValidationServiceServer
	store                        *Store
	log                          *slog.Logger
	bus                          *eventbus.Bus
	productCalculationClient     catalogv1.ProductCalculationServiceClient
	categoryValidationClient     catalogv1.CategoryValidationServiceClient
	conn                         *grpc.ClientConn
	metricSeriesValidationClient metricsv1.MetricSeriesValidationServiceClient
	validateProductFn            func(context.Context, *catalogv1.ValidateProductRequest, ...grpc.CallOption) (*catalogv1.ValidateProductResponse, error)
	venueClient                  venuesv1.VenueServiceClient
	featureFlagValidationClient  featureflagsv1.FeatureFlagValidationServiceClient
	warehouseQueryClient         warehousingv1.WarehouseQueryServiceClient
	orderNoteCalculationClient   ordersv1.OrderNoteCalculationServiceClient
}

// NewProductVariantValidationServer wires a ProductVariantValidationServer.
func NewProductVariantValidationServer(deps platform.Deps) *ProductVariantValidationServer {
	productValidation := catalogv1.NewProductValidationServiceClient(deps.Conn)
	return &ProductVariantValidationServer{
		store:                        NewStore(deps.DB),
		log:                          deps.Logger.With("service", "acme.catalog.v1.ProductVariantValidationService"),
		bus:                          deps.Bus,
		productCalculationClient:     catalogv1.NewProductCalculationServiceClient(deps.Conn),
		categoryValidationClient:     catalogv1.NewCategoryValidationServiceClient(deps.Conn),
		conn:                         deps.Conn,
		metricSeriesValidationClient: metricsv1.NewMetricSeriesValidationServiceClient(deps.Conn),
		validateProductFn:            productValidation.ValidateProduct,
		venueClient:                  venuesv1.NewVenueServiceClient(deps.Conn),
		featureFlagValidationClient:  featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn),
		warehouseQueryClient:         warehousingv1.NewWarehouseQueryServiceClient(deps.Conn),
		orderNoteCalculationClient:   ordersv1.NewOrderNoteCalculationServiceClient(deps.Conn),
	}
}

// ValidateProductVariant validates a product variant.
func (s *ProductVariantValidationServer) ValidateProductVariant(ctx context.Context, req *catalogv1.ValidateProductVariantRequest) (*catalogv1.ValidateProductVariantResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveProductVariant(ctx, req.GetContext().GetTenantId(), req.GetProductVariant()); err != nil {
		return nil, grpcx.Wrap(err, "save product variant")
	}
	s.log.InfoContext(ctx, "validate product variant", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductVariant().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "catalog.validate_product_variant.calculate_invoice_batch",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"details.destination.country_code": req.GetProductVariant().GetAddress().GetCountryCode(),
			"context.tenant_id":                req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	compareProductOptionsRequest := buildCompareProductOptionsRequestForValidateProductVariant(req)
	compareProductOptionsReply, err := s.productCalculationClient.CompareProductOptions(ctx, compareProductOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/CompareProductOptions")
	}
	validateCategoryRequest := &catalogv1.ValidateCategoryRequest{}
	validateCategoryRequest.Context = req.GetContext()
	validateCategoryRequest.Category = &catalogv1.Category{
		Children: []*catalogv1.Category{&catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Manager: &identityv1.UserRef{UserId: req.GetProductVariant().GetOwner().GetUserId()},
				Location: &commonv1.PostalAddress{
					PostalCode: compareProductOptionsReply.GetProduct().GetAddress().GetPostalCode(),
				},
			}},
		}},
	}
	validateCategoryReply, err := s.categoryValidationClient.ValidateCategory(ctx, validateCategoryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryValidationService/ValidateCategory")
	}
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeWarehousesRequest := &warehousingv1.SummarizeWarehousesRequest{
		Context:     req.GetContext(),
		Reason:      tenancy.Value(ctx, "reason"),
		Annotations: annotations,
		Details: &warehousingv1.SummarizeWarehousesRequest_Details{
			Filters: compareProductOptionsReply.GetResult().GetDetails(),
		},
	}
	summarizeWarehousesReply, err := grpcx.NewClient(s.conn, warehousingv1.NewWarehouseQueryServiceClient).SummarizeWarehouses(ctx, summarizeWarehousesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.WarehouseQueryService/SummarizeWarehouses")
	}
	return &catalogv1.ValidateProductVariantResponse{
		ProductVariant: &catalogv1.ProductVariant{
			Dimensions: &catalogv1.ProductVariantDimensions{
				Length: compareProductOptionsReply.GetProduct().GetDimensions().GetLength(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: validateCategoryReply.GetCategory().GetAudit().GetUpdatedAt(),
			},
			ProductVariantId: req.GetProductVariantId(),
			Specs:            req.GetProductVariant().GetSpecs(),
		},
		Outcome: &catalogv1.ValidateProductVariantResponse_Outcome{
			ReferenceId: validateCategoryReply.GetOutcome().GetReferenceId(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: summarizeWarehousesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// PreviewProductVariant previews a product variant.
func (s *ProductVariantValidationServer) PreviewProductVariant(ctx context.Context, req *catalogv1.PreviewProductVariantRequest) (*catalogv1.PreviewProductVariantResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveProductVariant(ctx, req.GetContext().GetTenantId(), req.GetProductVariant()); err != nil {
		return nil, grpcx.Wrap(err, "save product variant")
	}
	s.log.InfoContext(ctx, "preview product variant", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductVariant().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*catalogv1.PreviewProductVariantRequest_Details)
	source := req.GetProductVariant().GetDimensions().ProtoReflect()
	unit := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("unit"))).String()
	lintMetricSeriesesRequest := &metricsv1.LintMetricSeriesesRequest{}
	lintMetricSeriesesRequest.Context = req.GetContext()
	lintMetricSeriesesRequest.MetricSeries = &metricsv1.MetricSeries{
		Owner: &identityv1.UserRef{
			DisplayName: req.GetProductVariant().GetOwner().GetDisplayName(),
		},
		Audit: &commonv1.AuditInfo{
			CreatedBy: req.GetProductVariant().GetAudit().GetCreatedBy(),
		},
		Dimensions: &metricsv1.MetricSeriesDimensions{Unit: unit},
	}
	lintMetricSeriesesRequest.Details = &metricsv1.LintMetricSeriesesRequest_Details{
		Contact: &commonv1.ContactInfo{PreferredChannel: cloned.GetContact().GetPreferredChannel()},
	}
	lintMetricSeriesesReply, err := s.metricSeriesValidationClient.LintMetricSerieses(ctx, lintMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesValidationService/LintMetricSerieses")
	}
	validateProductRequest := &catalogv1.ValidateProductRequest{
		Context: req.GetContext(),
		Details: &catalogv1.ValidateProductRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: lintMetricSeriesesReply.GetMetricSeries().GetOwner().GetEmail(),
			},
		},
		Product: &catalogv1.Product{
			Dimensions: &catalogv1.ProductDimensions{Length: req.GetProductVariant().GetDimensions().GetLength()},
			Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{
				CountryCode: lintMetricSeriesesReply.GetMetricSeries().GetAddress().GetCountryCode(),
			}},
		},
	}
	validateProductReply, err := s.validateProductFn(ctx, validateProductRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductValidationService/ValidateProduct")
	}
	return &catalogv1.PreviewProductVariantResponse{
		Result:  &commonv1.OperationResult{Status: lintMetricSeriesesReply.GetResult().GetStatus()},
		Outcome: &catalogv1.PreviewProductVariantResponse_Outcome{Message: validateProductReply.GetResult().GetMessage()},
		ProductVariant: &catalogv1.ProductVariant{
			ProductVariantId: req.GetProductVariantId(),
			Name:             req.GetProductVariant().GetName(),
		},
	}, nil
}

// ExplainProductVariantDecision explains a product variant.
func (s *ProductVariantValidationServer) ExplainProductVariantDecision(ctx context.Context, req *catalogv1.ExplainProductVariantDecisionRequest) (*catalogv1.ExplainProductVariantDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainProductVariantDecision", "error", err)
		return nil, err
	}
	if err := s.store.SaveProductVariant(ctx, req.GetContext().GetTenantId(), req.GetProductVariant()); err != nil {
		return nil, grpcx.Wrap(err, "save product variant")
	}
	s.log.InfoContext(ctx, "explain product variant decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductVariant().GetAudit().GetEtag())
	cloneVenueReply, err := s.venueClient.CloneVenue(ctx, &venuesv1.CloneVenueRequest{
		Context: req.GetContext(),
		Venue: &venuesv1.Venue{
			Owner: &identityv1.UserRef{Email: req.GetProductVariant().GetOwner().GetEmail()},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetProductVariant().GetAudit().GetUpdatedBy(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueService/CloneVenue")
	}
	estimateProductRequest := &catalogv1.EstimateProductRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Components: map[string]*catalogv1.ProductComponent{"primary": &catalogv1.ProductComponent{
				Quantity:    req.GetProductVariant().GetComponents()["primary"].GetQuantity(),
				ComponentId: req.GetProductVariant().GetComponents()["primary"].GetComponentId(),
			}},
			Dimensions: &catalogv1.ProductDimensions{
				Length: cloneVenueReply.GetVenue().GetDimensions().GetLength(),
				Width:  cloneVenueReply.GetVenue().GetDimensions().GetWidth(),
			},
		},
	}
	estimateProductReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*catalogv1.EstimateProductResponse, error) {
		return s.productCalculationClient.EstimateProduct(ctx, estimateProductRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/EstimateProduct")
	}
	return &catalogv1.ExplainProductVariantDecisionResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: cloneVenueReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		ProductVariant: &catalogv1.ProductVariant{
			Code:             estimateProductReply.GetProduct().GetCode(),
			ProductVariantId: req.GetProductVariantId(),
			Specs:            req.GetProductVariant().GetSpecs(),
		},
		Outcome: &catalogv1.ExplainProductVariantDecisionResponse_Outcome{Message: estimateProductReply.GetResult().GetMessage()},
	}, nil
}

// ScoreProductVariant scores a product variant.
func (s *ProductVariantValidationServer) ScoreProductVariant(ctx context.Context, req *catalogv1.ScoreProductVariantRequest) (*catalogv1.ScoreProductVariantResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveProductVariant(ctx, req.GetContext().GetTenantId(), req.GetProductVariant()); err != nil {
		return nil, grpcx.Wrap(err, "save product variant")
	}
	s.log.InfoContext(ctx, "score product variant", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductVariant().GetAudit().GetEtag())
	var holder struct{ Region string }
	if record := req.GetDetails().GetDestination(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Region"); err != nil {
			s.log.WarnContext(ctx, "copy region", "error", err)
		}
	}
	explainCategoryDecisionReply, err := s.categoryValidationClient.ExplainCategoryDecision(ctx, &catalogv1.ExplainCategoryDecisionRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Audit: &commonv1.AuditInfo{
						CreatedBy: req.GetProductVariant().GetAudit().GetCreatedBy(),
					},
					Location: &commonv1.PostalAddress{
						Recipient: req.GetDetails().GetDestination().GetRecipient(),
						Region:    holder.Region,
					},
				}},
				Audit: &commonv1.AuditInfo{
					CreatedAt: req.GetProductVariant().GetAudit().GetCreatedAt(),
				},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryValidationService/ExplainCategoryDecision")
	}
	return &catalogv1.ScoreProductVariantResponse{
		ProductVariant: &catalogv1.ProductVariant{
			Audit: &commonv1.AuditInfo{
				Etag: explainCategoryDecisionReply.GetCategory().GetAudit().GetEtag(),
			},
			ProductVariantId: req.GetProductVariantId(),
			Capacity:         req.GetProductVariant().GetCapacity(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LintProductVariants lints a product variant.
func (s *ProductVariantValidationServer) LintProductVariants(ctx context.Context, req *catalogv1.LintProductVariantsRequest) (*catalogv1.LintProductVariantsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveProductVariant(ctx, req.GetContext().GetTenantId(), req.GetProductVariant()); err != nil {
		return nil, grpcx.Wrap(err, "save product variant")
	}
	s.log.InfoContext(ctx, "lint product variants", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductVariant().GetAudit().GetEtag())
	explainFeatureFlagDecisionRequest := buildExplainFeatureFlagDecisionRequestForLintProductVariants(req)
	explainFeatureFlagDecisionReply, err := s.featureFlagValidationClient.ExplainFeatureFlagDecision(ctx, explainFeatureFlagDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/ExplainFeatureFlagDecision")
	}
	var region string
	switch choice := req.GetProductVariant().GetLocator().(type) {
	case *catalogv1.ProductVariant_Address:
		region = choice.Address.GetRegion()
	case *catalogv1.ProductVariant_FacilityCode:
		region = choice.FacilityCode
	}
	browseWarehousesReply, err := s.warehouseQueryClient.BrowseWarehouses(ctx, &warehousingv1.BrowseWarehousesRequest{
		Context: req.GetContext(),
		Filter: &warehousingv1.BrowseWarehousesRequest_Filter{
			Region:       region,
			CreatedAfter: explainFeatureFlagDecisionReply.GetFeatureFlag().GetAudit().GetCreatedAt(),
			OwnerId:      explainFeatureFlagDecisionReply.GetFeatureFlag().GetOwner().GetUserId(),
			Labels:       explainFeatureFlagDecisionReply.GetResult().GetDetails(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.WarehouseQueryService/BrowseWarehouses")
	}
	explainProductCalculationReply, err := s.productCalculationClient.ExplainProductCalculation(ctx, &catalogv1.ExplainProductCalculationRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Dimensions: &catalogv1.ProductDimensions{
				Weight: req.GetProductVariant().GetDimensions().GetWeight(),
				Height: req.GetProductVariant().GetDimensions().GetHeight(),
			},
			Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{Lines: req.GetProductVariant().GetAddress().GetLines()}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/ExplainProductCalculation")
	}
	packed, err := anypb.New(req.GetProductVariant())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	compareOrderNoteOptionsRequest := newCompareOrderNoteOptionsRequest(withCompareOrderNoteOptionsContext(req.GetContext()), withCompareOrderNoteOptionsOrderNote(&ordersv1.OrderNote{
		Subject: &ordersv1.OrderRef{
			DisplayName: req.GetProductVariant().GetOwner().GetDisplayName(),
		},
		Detail: packed,
	}), withCompareOrderNoteOptionsDetails(&ordersv1.CompareOrderNoteOptionsRequest_Details{
		Destination: &commonv1.PostalAddress{
			Locality: req.GetProductVariant().GetAddress().GetLocality(),
		},
	}))
	compareOrderNoteOptionsReply, err := s.orderNoteCalculationClient.CompareOrderNoteOptions(ctx, compareOrderNoteOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteCalculationService/CompareOrderNoteOptions")
	}
	return &catalogv1.LintProductVariantsResponse{
		Outcome: &catalogv1.LintProductVariantsResponse_Outcome{
			Message: explainFeatureFlagDecisionReply.GetOutcome().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			Message:       browseWarehousesReply.GetResult().GetMessage(),
			CorrelationId: browseWarehousesReply.GetResult().GetCorrelationId(),
			Details:       compareOrderNoteOptionsReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		ProductVariant: &catalogv1.ProductVariant{
			Dimensions: &catalogv1.ProductVariantDimensions{
				Unit:   explainProductCalculationReply.GetProduct().GetDimensions().GetUnit(),
				Height: explainProductCalculationReply.GetProduct().GetDimensions().GetHeight(),
			},
			ProductVariantId: req.GetProductVariantId(),
			Owner:            req.GetProductVariant().GetOwner(),
		},
	}, nil
}

// GetProductVariantRules returns a product variant.
func (s *ProductVariantValidationServer) GetProductVariantRules(ctx context.Context, req *catalogv1.GetProductVariantRulesRequest) (*catalogv1.GetProductVariantRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product_variant", req.GetProductVariantId(), "get_product_variant_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get product variant rules", "tenant", req.GetContext().GetTenantId())
	return &catalogv1.GetProductVariantRulesResponse{
		ProductVariant: &catalogv1.ProductVariant{ProductVariantId: req.GetProductVariantId()},
		Result:         &commonv1.OperationResult{Status: "ok"},
	}, nil
}
