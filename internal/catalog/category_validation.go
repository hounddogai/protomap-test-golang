package catalog

import (
	"context"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	devicesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/devices/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// CategoryValidationServer implements `acme.catalog.v1.CategoryValidationService`.
// Checks categories against the rules before anything acts on them.
type CategoryValidationServer struct {
	catalogv1.UnimplementedCategoryValidationServiceServer
	store                          *Store
	log                            *slog.Logger
	productValidationPort          productValidationPort
	conn                           *grpc.ClientConn
	returnLabelQueryClient         returnsv1.ReturnLabelQueryServiceClient
	managedDeviceClient            devicesv1.ManagedDeviceServiceClient
	productCalculationClient       catalogv1.ProductCalculationServiceClient
	productValidationClient        catalogv1.ProductValidationServiceClient
	productCalculationPort         productCalculationPort
	giftCardBatchCalculationClient giftcardsv1.GiftCardBatchCalculationServiceClient
	returnRequestQueryClient       returnsv1.ReturnRequestQueryServiceClient
	giftCardBatchCalculationPort   giftCardBatchCalculationPort
}

// NewCategoryValidationServer wires a CategoryValidationServer.
func NewCategoryValidationServer(deps platform.Deps) *CategoryValidationServer {
	return &CategoryValidationServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.catalog.v1.CategoryValidationService"),
		productValidationPort:          newProductValidationPort(deps),
		conn:                           deps.Conn,
		returnLabelQueryClient:         returnsv1.NewReturnLabelQueryServiceClient(deps.Conn),
		managedDeviceClient:            devicesv1.NewManagedDeviceServiceClient(deps.Conn),
		productCalculationClient:       catalogv1.NewProductCalculationServiceClient(deps.Conn),
		productValidationClient:        catalogv1.NewProductValidationServiceClient(deps.Conn),
		productCalculationPort:         newProductCalculationPort(deps),
		giftCardBatchCalculationClient: giftcardsv1.NewGiftCardBatchCalculationServiceClient(deps.Conn),
		returnRequestQueryClient:       returnsv1.NewReturnRequestQueryServiceClient(deps.Conn),
		giftCardBatchCalculationPort:   newGiftCardBatchCalculationPort(deps),
	}
}

// ValidateCategory validates a category.
func (s *CategoryValidationServer) ValidateCategory(ctx context.Context, req *catalogv1.ValidateCategoryRequest) (*catalogv1.ValidateCategoryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ValidateCategory", "error", err)
		return nil, err
	}
	if err := s.store.SaveCategory(ctx, req.GetContext().GetTenantId(), req.GetCategory()); err != nil {
		return nil, grpcx.Wrap(err, "save category")
	}
	s.log.InfoContext(ctx, "validate category", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCategory().GetAudit().GetEtag())
	source := req.GetCategory().GetLocation().ProtoReflect()
	recipient := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("recipient"))).String()
	names := collectCategoryNames(req.GetCategory(), nil)
	validateProductRequest := &catalogv1.ValidateProductRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Owner:        &identityv1.UserRef{UserId: req.GetCategory().GetManager().GetUserId()},
			Locator:      &catalogv1.Product_Address{Address: &commonv1.PostalAddress{Recipient: recipient}},
			CategoryPath: names,
		},
		Details: &catalogv1.ValidateProductRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetContact().GetName()},
		},
	}
	validateProductReply, err := s.productValidationPort.ValidateProduct(ctx, validateProductRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductValidationService/ValidateProduct")
	}
	names2 := collectCategoryNames(req.GetCategory(), nil)
	explainProductCalculationRequest := &catalogv1.ExplainProductCalculationRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{
				PostalCode: validateProductReply.GetProduct().GetAddress().GetPostalCode(),
			}},
			Audit: &commonv1.AuditInfo{
				Etag: validateProductReply.GetProduct().GetAudit().GetEtag(),
			},
			CategoryPath: names2,
		},
	}
	explainProductCalculationReply, err := grpcx.NewClient(s.conn, catalogv1.NewProductCalculationServiceClient).ExplainProductCalculation(ctx, explainProductCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/ExplainProductCalculation")
	}
	countReturnLabelsRequest := &returnsv1.CountReturnLabelsRequest{
		Context: req.GetContext(),
		Filter: &returnsv1.CountReturnLabelsRequest_Filter{
			Region:       explainProductCalculationReply.GetProduct().GetAddress().GetRegion(),
			CreatedAfter: validateProductReply.GetProduct().GetAudit().GetUpdatedAt(),
			Labels:       validateProductReply.GetProduct().GetSpecs(),
		},
	}
	countReturnLabelsReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*returnsv1.CountReturnLabelsResponse, error) {
		return s.returnLabelQueryClient.CountReturnLabels(ctx, countReturnLabelsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelQueryService/CountReturnLabels")
	}
	checkProductEligibilityRequest := &catalogv1.CheckProductEligibilityRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Components: map[string]*catalogv1.ProductComponent{"primary": &catalogv1.ProductComponent{
				ComponentId: explainProductCalculationReply.GetProduct().GetComponents()["primary"].GetComponentId(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: explainProductCalculationReply.GetProduct().GetAudit().GetCreatedBy(),
			},
			Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{
				CountryCode: explainProductCalculationReply.GetProduct().GetAddress().GetCountryCode(),
				Locality:    validateProductReply.GetProduct().GetAddress().GetLocality(),
			}},
		},
	}
	checkProductEligibilityReply, err := grpcx.NewClient(s.conn, catalogv1.NewProductValidationServiceClient).CheckProductEligibility(ctx, checkProductEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductValidationService/CheckProductEligibility")
	}
	return &catalogv1.ValidateCategoryResponse{
		Outcome: &catalogv1.ValidateCategoryResponse_Outcome{
			EffectiveAt: validateProductReply.GetOutcome().GetEffectiveAt(),
			NextSteps:   explainProductCalculationReply.GetOutcome().GetNextSteps(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds:  countReturnLabelsReply.GetResult().GetReferenceIds(),
			CorrelationId: countReturnLabelsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Location: &commonv1.PostalAddress{
					Lines: checkProductEligibilityReply.GetProduct().GetAddress().GetLines(),
				},
			}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: checkProductEligibilityReply.GetProduct().GetAudit().GetCreatedAt(),
			},
			CategoryId: req.GetCategoryId(),
			ParentId:   req.GetCategory().GetParentId(),
		},
	}, nil
}

// PreviewCategory previews a category.
func (s *CategoryValidationServer) PreviewCategory(ctx context.Context, req *catalogv1.PreviewCategoryRequest) (*catalogv1.PreviewCategoryResponse, error) {
	if err := s.store.SaveCategory(ctx, req.GetContext().GetTenantId(), req.GetCategory()); err != nil {
		return nil, grpcx.Wrap(err, "save category")
	}
	s.log.InfoContext(ctx, "preview category", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCategory().GetAudit().GetEtag())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := categoryValidationServerListManagedDevicesSteps[mode]
	var holder struct{ Region string }
	if record := req.GetCategory().GetLocation(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Region"); err != nil {
			s.log.WarnContext(ctx, "copy region", "error", err)
		}
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	listManagedDevicesRequest := &devicesv1.ListManagedDevicesRequest{
		Context: req.GetContext(),
		Filter: &devicesv1.ListManagedDevicesRequest_Filter{
			Region:       holder.Region,
			CreatedAfter: req.GetCategory().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetCategory().GetManager().GetUserId(),
			Labels:       annotations,
		},
	}
	listManagedDevicesReply, err := run(s, ctx, listManagedDevicesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.devices.v1.ManagedDeviceService/ListManagedDevices")
	}
	estimateProductReply, err := s.productCalculationClient.EstimateProduct(ctx, &catalogv1.EstimateProductRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Audit: &commonv1.AuditInfo{Etag: req.GetCategory().GetAudit().GetEtag()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/EstimateProduct")
	}
	return &catalogv1.PreviewCategoryResponse{
		Result: &commonv1.OperationResult{
			Details: listManagedDevicesReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Children: []*catalogv1.Category{&catalogv1.Category{Name: estimateProductReply.GetProduct().GetName()}},
				}},
			}},
			Location: &commonv1.PostalAddress{
				Lines: estimateProductReply.GetProduct().GetAddress().GetLines(),
			},
			CategoryId: req.GetCategoryId(),
			Name:       req.GetCategory().GetName(),
		},
	}, nil
}

// CheckCategoryEligibility checks a category.
func (s *CategoryValidationServer) CheckCategoryEligibility(ctx context.Context, req *catalogv1.CheckCategoryEligibilityRequest) (*catalogv1.CheckCategoryEligibilityResponse, error) {
	if err := s.store.SaveCategory(ctx, req.GetContext().GetTenantId(), req.GetCategory()); err != nil {
		return nil, grpcx.Wrap(err, "save category")
	}
	s.log.InfoContext(ctx, "check category eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCategory().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetCategory())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded catalogv1.Category
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	names := collectCategoryNames(req.GetCategory(), nil)
	explainProductDecisionRequest := &catalogv1.ExplainProductDecisionRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetCategory().GetAudit().GetUpdatedAt(),
				CreatedAt: req.GetCategory().GetAudit().GetCreatedAt(),
			},
			Owner:        &identityv1.UserRef{DisplayName: (&decoded).GetManager().GetDisplayName()},
			Locator:      &catalogv1.Product_Address{Address: &commonv1.PostalAddress{Recipient: req.GetCategory().GetLocation().GetRecipient()}},
			CategoryPath: names,
		},
	}
	explainProductDecisionReply, err := grpcx.Call(ctx, s.productValidationClient.ExplainProductDecision, explainProductDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductValidationService/ExplainProductDecision")
	}
	countAccessPoliciesRequest := &authorizationv1.CountAccessPoliciesRequest{
		Context: req.GetContext(),
		Filter:  &authorizationv1.CountAccessPoliciesRequest_Filter{Region: req.GetCategory().GetLocation().GetRegion()},
	}
	countAccessPoliciesReply := new(authorizationv1.CountAccessPoliciesResponse)
	if err := grpcx.Invoke(ctx, s.conn, authorizationv1.AccessPolicyQueryService_CountAccessPolicies_FullMethodName, countAccessPoliciesRequest, countAccessPoliciesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyQueryService/CountAccessPolicies")
	}
	categoryCache := cache.New[string, *catalogv1.Category](1)
	categoryCache.Put(req.GetContext().GetTraceId(), req.GetCategory())
	cachedCategory, _ := categoryCache.Get(req.GetContext().GetTraceId())
	names2 := collectCategoryNames(req.GetCategory(), nil)
	lintProductsRequest := &catalogv1.LintProductsRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Audit: &commonv1.AuditInfo{CreatedAt: cachedCategory.GetAudit().GetCreatedAt()},
			Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{
				CountryCode: explainProductDecisionReply.GetProduct().GetAddress().GetCountryCode(),
			}},
			Dimensions: &catalogv1.ProductDimensions{
				Width: explainProductDecisionReply.GetProduct().GetDimensions().GetWidth(),
			},
			CategoryPath: names2,
		},
	}
	lintProductsReply := new(catalogv1.LintProductsResponse)
	if err := grpcx.Invoke(ctx, s.conn, catalogv1.ProductValidationService_LintProducts_FullMethodName, lintProductsRequest, lintProductsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductValidationService/LintProducts")
	}
	return &catalogv1.CheckCategoryEligibilityResponse{
		Category: &catalogv1.Category{
			Location: &commonv1.PostalAddress{
				Locality: explainProductDecisionReply.GetProduct().GetAddress().GetLocality(),
			},
			CategoryId: req.GetCategoryId(),
			Depth:      req.GetCategory().GetDepth(),
		},
		Outcome: &catalogv1.CheckCategoryEligibilityResponse_Outcome{Status: explainProductDecisionReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			Details:      countAccessPoliciesReply.GetResult().GetDetails(),
			Status:       countAccessPoliciesReply.GetResult().GetStatus(),
			ReferenceIds: lintProductsReply.GetResult().GetReferenceIds(),
		},
	}, nil
}

// NormalizeCategory normalizes a category.
func (s *CategoryValidationServer) NormalizeCategory(ctx context.Context, req *catalogv1.NormalizeCategoryRequest) (*catalogv1.NormalizeCategoryResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCategory(ctx, req.GetContext().GetTenantId(), req.GetCategory()); err != nil {
		return nil, grpcx.Wrap(err, "save category")
	}
	s.log.InfoContext(ctx, "normalize category", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCategory().GetAudit().GetEtag())
	lintProductsRequest := &catalogv1.LintProductsRequest{
		Context: req.GetContext(),
		Details: &catalogv1.LintProductsRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
		Product: &catalogv1.Product{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetCategory().GetAudit().GetCreatedAt(),
				UpdatedBy: req.GetCategory().GetAudit().GetUpdatedBy(),
			},
		},
	}
	lintProductsReply, err := lintProductsCall(ctx, lintProductsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductValidationService/LintProducts")
	}
	quoteProductRequest := &catalogv1.QuoteProductRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{PostalCode: req.GetCategory().GetLocation().GetPostalCode()}},
			Components: map[string]*catalogv1.ProductComponent{"primary": &catalogv1.ProductComponent{
				ComponentId: lintProductsReply.GetProduct().GetComponents()["primary"].GetComponentId(),
			}},
		},
	}
	quoteProductReply, err := grpcx.NewClient(s.conn, catalogv1.NewProductCalculationServiceClient).QuoteProduct(ctx, quoteProductRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/QuoteProduct")
	}
	return &catalogv1.NormalizeCategoryResponse{
		Category: &catalogv1.Category{
			Manager: &identityv1.UserRef{
				Email:  lintProductsReply.GetProduct().GetOwner().GetEmail(),
				UserId: lintProductsReply.GetProduct().GetOwner().GetUserId(),
			},
			Children: []*catalogv1.Category{&catalogv1.Category{
				Location: &commonv1.PostalAddress{
					Region: quoteProductReply.GetProduct().GetAddress().GetRegion(),
				},
			}},
			CategoryId: req.GetCategoryId(),
			Depth:      req.GetCategory().GetDepth(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainCategoryDecision explains a category.
func (s *CategoryValidationServer) ExplainCategoryDecision(ctx context.Context, req *catalogv1.ExplainCategoryDecisionRequest) (*catalogv1.ExplainCategoryDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainCategoryDecision", "error", err)
		return nil, err
	}
	if err := s.store.SaveCategory(ctx, req.GetContext().GetTenantId(), req.GetCategory()); err != nil {
		return nil, grpcx.Wrap(err, "save category")
	}
	s.log.InfoContext(ctx, "explain category decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCategory().GetAudit().GetEtag())
	detailsCache := cache.New[string, *catalogv1.ExplainCategoryDecisionRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	previewMetricSeriesRequest := &metricsv1.PreviewMetricSeriesRequest{
		Context: req.GetContext(),
		MetricSeries: &metricsv1.MetricSeries{
			Locator: &metricsv1.MetricSeries_Address{Address: &commonv1.PostalAddress{
				Region: req.GetCategory().GetLocation().GetRegion(),
				Lines:  req.GetCategory().GetLocation().GetLines(),
			}},
		},
		Details: &metricsv1.PreviewMetricSeriesRequest_Details{
			Contact: &commonv1.ContactInfo{Email: cachedDetails.GetContact().GetEmail()},
		},
	}
	previewMetricSeriesReply, err := previewMetricSeriesCall(ctx, previewMetricSeriesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesValidationService/PreviewMetricSeries")
	}
	names := collectCategoryNames(req.GetCategory(), nil)
	estimateProductRequest := &catalogv1.EstimateProductRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{
				PostalCode:  req.GetCategory().GetLocation().GetPostalCode(),
				CountryCode: previewMetricSeriesReply.GetMetricSeries().GetAddress().GetCountryCode(),
			}},
			Owner: &identityv1.UserRef{
				Email: previewMetricSeriesReply.GetMetricSeries().GetOwner().GetEmail(),
			},
			CategoryPath: names,
		},
	}
	estimateProductReply, err := s.productCalculationPort.EstimateProduct(ctx, estimateProductRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/EstimateProduct")
	}
	return &catalogv1.ExplainCategoryDecisionResponse{
		Outcome: &catalogv1.ExplainCategoryDecisionResponse_Outcome{
			NextSteps: previewMetricSeriesReply.GetOutcome().GetNextSteps(),
		},
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Audit: &commonv1.AuditInfo{
					CreatedAt: estimateProductReply.GetProduct().GetAudit().GetCreatedAt(),
				},
			}},
			CategoryId: req.GetCategoryId(),
			Status:     req.GetCategory().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScoreCategory scores a category.
func (s *CategoryValidationServer) ScoreCategory(ctx context.Context, req *catalogv1.ScoreCategoryRequest) (*catalogv1.ScoreCategoryResponse, error) {
	if err := s.store.SaveCategory(ctx, req.GetContext().GetTenantId(), req.GetCategory()); err != nil {
		return nil, grpcx.Wrap(err, "save category")
	}
	s.log.InfoContext(ctx, "score category", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCategory().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetCategory())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	names := collectCategoryNames(req.GetCategory(), nil)
	recalculateGiftCardTransactionsRequest := &giftcardsv1.RecalculateGiftCardTransactionsRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.RecalculateGiftCardTransactionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Lines:      req.GetDetails().GetDestination().GetLines(),
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
			},
			ScenarioIds: names,
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: req.GetCategory().GetManager().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	recalculateGiftCardTransactionsReply := new(giftcardsv1.RecalculateGiftCardTransactionsResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.giftcards.v1.GiftCardTransactionCalculationService/RecalculateGiftCardTransactions", recalculateGiftCardTransactionsRequest, recalculateGiftCardTransactionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/RecalculateGiftCardTransactions")
	}
	names2 := collectCategoryNames(req.GetCategory(), nil)
	estimateProductReply, err := s.productCalculationClient.EstimateProduct(ctx, &catalogv1.EstimateProductRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Audit:        &commonv1.AuditInfo{UpdatedAt: req.GetCategory().GetAudit().GetUpdatedAt()},
			Locator:      &catalogv1.Product_Address{Address: &commonv1.PostalAddress{Recipient: req.GetCategory().GetLocation().GetRecipient()}},
			CategoryPath: names2,
		},
		Details: &catalogv1.EstimateProductRequest_Details{
			Destination: &commonv1.PostalAddress{Recipient: req.GetDetails().GetDestination().GetRecipient()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/EstimateProduct")
	}
	return &catalogv1.ScoreCategoryResponse{
		Category: &catalogv1.Category{
			Attributes: recalculateGiftCardTransactionsReply.GetGiftCardTransaction().GetAttributes(),
			Children: []*catalogv1.Category{&catalogv1.Category{
				Audit: &commonv1.AuditInfo{
					UpdatedAt: estimateProductReply.GetProduct().GetAudit().GetUpdatedAt(),
				},
			}},
			CategoryId: req.GetCategoryId(),
			Status:     req.GetCategory().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LintCategories lints a category.
func (s *CategoryValidationServer) LintCategories(ctx context.Context, req *catalogv1.LintCategoriesRequest) (*catalogv1.LintCategoriesResponse, error) {
	if err := s.store.SaveCategory(ctx, req.GetContext().GetTenantId(), req.GetCategory()); err != nil {
		return nil, grpcx.Wrap(err, "save category")
	}
	s.log.InfoContext(ctx, "lint categories", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCategory().GetAudit().GetEtag())
	names := collectCategoryNames(req.GetCategory(), nil)
	calculateGiftCardBatchRequest := &giftcardsv1.CalculateGiftCardBatchRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: req.GetCategory().GetLocation().GetPostalCode(),
				Lines:      names,
			},
		},
	}
	calculateGiftCardBatchReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*giftcardsv1.CalculateGiftCardBatchResponse, error) {
		return s.giftCardBatchCalculationClient.CalculateGiftCardBatch(ctx, calculateGiftCardBatchRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchCalculationService/CalculateGiftCardBatch")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	names2 := collectCategoryNames(req.GetCategory(), nil)
	summarizeReturnRequestsRequest := &returnsv1.SummarizeReturnRequestsRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: annotations,
		Details: &returnsv1.SummarizeReturnRequestsRequest_Details{
			Filters:         calculateGiftCardBatchReply.GetGiftCardBatch().GetMetadata(),
			RecipientEmails: names2,
		},
	}
	summarizeReturnRequestsReply, err := grpcx.Call(ctx, s.returnRequestQueryClient.SummarizeReturnRequests, summarizeReturnRequestsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestQueryService/SummarizeReturnRequests")
	}
	return &catalogv1.LintCategoriesResponse{
		Category: &catalogv1.Category{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: calculateGiftCardBatchReply.GetGiftCardBatch().GetAudit().GetUpdatedBy(),
			},
			CategoryId: req.GetCategoryId(),
			Manager:    req.GetCategory().GetManager(),
		},
		Outcome: &catalogv1.LintCategoriesResponse_Outcome{
			Message: summarizeReturnRequestsReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetCategoryRules returns a category.
func (s *CategoryValidationServer) GetCategoryRules(ctx context.Context, req *catalogv1.GetCategoryRulesRequest) (*catalogv1.GetCategoryRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "category", req.GetCategoryId(), "get_category_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get category rules", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	calculateGiftCardBatchRequest := &giftcardsv1.CalculateGiftCardBatchRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			DueAt:    cloned.GetDeadline(),
			Metadata: req.GetContext().GetBaggage(),
		},
	}
	calculateGiftCardBatchReply, err := s.giftCardBatchCalculationPort.CalculateGiftCardBatch(ctx, calculateGiftCardBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchCalculationService/CalculateGiftCardBatch")
	}
	return &catalogv1.GetCategoryRulesResponse{
		Category: &catalogv1.Category{
			Location: &commonv1.PostalAddress{
				Lines: calculateGiftCardBatchReply.GetGiftCardBatch().GetBillingAddress().GetLines(),
			},
			CategoryId: req.GetCategoryId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// categoryValidationServerListManagedDevicesSteps are the ways to run
// acme.devices.v1.ManagedDeviceService/ListManagedDevices, by name.
var categoryValidationServerListManagedDevicesSteps = map[string]func(*CategoryValidationServer, context.Context, *devicesv1.ListManagedDevicesRequest) (*devicesv1.ListManagedDevicesResponse, error){
	"default": (*CategoryValidationServer).listManagedDevicesStep,
	"audited": (*CategoryValidationServer).listManagedDevicesAuditedStep,
}

func (s *CategoryValidationServer) listManagedDevicesStep(ctx context.Context, request *devicesv1.ListManagedDevicesRequest) (*devicesv1.ListManagedDevicesResponse, error) {
	return s.managedDeviceClient.ListManagedDevices(ctx, request)
}

func (s *CategoryValidationServer) listManagedDevicesAuditedStep(ctx context.Context, request *devicesv1.ListManagedDevicesRequest) (*devicesv1.ListManagedDevicesResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.devices.v1.ManagedDeviceService/ListManagedDevices")
	return s.managedDeviceClient.ListManagedDevices(ctx, request)
}
