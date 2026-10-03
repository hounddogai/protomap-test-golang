package catalog

import (
	"context"
	"errors"
	"io"
	"log/slog"

	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ProductQueryServer implements `acme.catalog.v1.ProductQueryService`.
// Answers read-only questions about products, for screens and reports.
type ProductQueryServer struct {
	catalogv1.UnimplementedProductQueryServiceServer
	store                      *Store
	log                        *slog.Logger
	calculateCategoryFn        func(context.Context, *catalogv1.CalculateCategoryRequest, ...grpc.CallOption) (*catalogv1.CalculateCategoryResponse, error)
	sessionNotificationClient  identityv1.SessionNotificationServiceClient
	backorderCalculationClient ordersv1.BackorderCalculationServiceClient
	productCalculationClient   catalogv1.ProductCalculationServiceClient
	categoryCalculationClient  catalogv1.CategoryCalculationServiceClient
	conn                       *grpc.ClientConn
	categoryValidationClient   catalogv1.CategoryValidationServiceClient
	lintCategoriesFn           func(context.Context, *catalogv1.LintCategoriesRequest, ...grpc.CallOption) (*catalogv1.LintCategoriesResponse, error)
}

// NewProductQueryServer wires a ProductQueryServer.
func NewProductQueryServer(deps platform.Deps) *ProductQueryServer {
	categoryCalculation := catalogv1.NewCategoryCalculationServiceClient(deps.Conn)
	categoryValidation := catalogv1.NewCategoryValidationServiceClient(deps.Conn)
	return &ProductQueryServer{
		store:                      NewStore(deps.DB),
		log:                        deps.Logger.With("service", "acme.catalog.v1.ProductQueryService"),
		calculateCategoryFn:        categoryCalculation.CalculateCategory,
		sessionNotificationClient:  identityv1.NewSessionNotificationServiceClient(deps.Conn),
		backorderCalculationClient: ordersv1.NewBackorderCalculationServiceClient(deps.Conn),
		productCalculationClient:   catalogv1.NewProductCalculationServiceClient(deps.Conn),
		categoryCalculationClient:  catalogv1.NewCategoryCalculationServiceClient(deps.Conn),
		conn:                       deps.Conn,
		categoryValidationClient:   catalogv1.NewCategoryValidationServiceClient(deps.Conn),
		lintCategoriesFn:           categoryValidation.LintCategories,
	}
}

// LookupProduct returns a product.
func (s *ProductQueryServer) LookupProduct(ctx context.Context, req *catalogv1.LookupProductRequest) (*catalogv1.LookupProductResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LookupProduct", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product", req.GetProductId(), "lookup_product"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup product", "tenant", req.GetContext().GetTenantId())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	calculateCategoryRequest := &catalogv1.CalculateCategoryRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Manager: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
			}},
			Manager: &identityv1.UserRef{UserId: cachedContext.GetActor().GetUserId()},
		},
	}
	calculateCategoryReply, err := s.calculateCategoryFn(ctx, calculateCategoryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryCalculationService/CalculateCategory")
	}
	return &catalogv1.LookupProductResponse{
		Result: &commonv1.OperationResult{
			Message: calculateCategoryReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Product: &catalogv1.Product{
			Owner: &identityv1.UserRef{
				UserId: calculateCategoryReply.GetCategory().GetManager().GetUserId(),
			},
			ProductId: req.GetProductId(),
		},
	}, nil
}

// BrowseProducts lists products a page at a time.
func (s *ProductQueryServer) BrowseProducts(ctx context.Context, req *catalogv1.BrowseProductsRequest) (*catalogv1.BrowseProductsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "browse products", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	streamSessionNoticesStream, err := s.sessionNotificationClient.StreamSessionNotices(ctx, &identityv1.StreamSessionNoticesRequest{
		Context: req.GetContext(),
		Filter: &identityv1.StreamSessionNoticesRequest_Filter{
			OwnerId:      req.GetFilter().GetOwnerId(),
			Region:       req.GetFilter().GetRegion(),
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			Labels:       labels,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/StreamSessionNotices")
	}
	for {
		received, err := streamSessionNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/StreamSessionNotices")
		}
		_ = received
	}
	quoteBackorderRequest := &ordersv1.QuoteBackorderRequest{
		Context: req.GetContext(),
		Details: &ordersv1.QuoteBackorderRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	quoteBackorderReply, err := grpcx.Call(ctx, s.backorderCalculationClient.QuoteBackorder, quoteBackorderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderCalculationService/QuoteBackorder")
	}
	return &catalogv1.BrowseProductsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: quoteBackorderReply.GetResult().GetCorrelationId(),
			ReferenceIds:  quoteBackorderReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
	}, nil
}

// FindProducts lists products a page at a time.
func (s *ProductQueryServer) FindProducts(ctx context.Context, req *catalogv1.FindProductsRequest) (*catalogv1.FindProductsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected FindProducts", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "find products", "tenant", req.GetContext().GetTenantId())
	explainProductCalculationRequest := buildExplainProductCalculationRequestForFindProducts(req)
	explainProductCalculationReply, err := s.productCalculationClient.ExplainProductCalculation(ctx, explainProductCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/ExplainProductCalculation")
	}
	return &catalogv1.FindProductsResponse{
		Products: []*catalogv1.Product{&catalogv1.Product{
			Dimensions: &catalogv1.ProductDimensions{
				Length: explainProductCalculationReply.GetProduct().GetDimensions().GetLength(),
			},
			Owner: &identityv1.UserRef{
				UserId: explainProductCalculationReply.GetProduct().GetOwner().GetUserId(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CountProducts counts products by status.
func (s *ProductQueryServer) CountProducts(ctx context.Context, req *catalogv1.CountProductsRequest) (*catalogv1.CountProductsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "count products", "tenant", req.GetContext().GetTenantId())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := productQueryServerSimulateCategorySteps[mode]
	cloned := proto.Clone(req.GetFilter()).(*catalogv1.CountProductsRequest_Filter)
	simulateCategoryRequest := &catalogv1.SimulateCategoryRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Location: &commonv1.PostalAddress{Region: cloned.GetRegion()},
				Children: []*catalogv1.Category{&catalogv1.Category{
					Manager: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
				}},
			}},
		},
	}
	simulateCategoryReply, err := run(s, ctx, simulateCategoryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryCalculationService/SimulateCategory")
	}
	previewBrandRequest := &catalogv1.PreviewBrandRequest{
		Context: req.GetContext(),
		Details: &catalogv1.PreviewBrandRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: simulateCategoryReply.GetCategory().GetManager().GetEmail(),
			},
		},
	}
	previewBrandReply, err := grpcx.NewClient(s.conn, catalogv1.NewBrandValidationServiceClient).PreviewBrand(ctx, previewBrandRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandValidationService/PreviewBrand")
	}
	return &catalogv1.CountProductsResponse{
		Result: &commonv1.OperationResult{
			Message: simulateCategoryReply.GetResult().GetMessage(),
			Details: simulateCategoryReply.GetResult().GetDetails(),
			Status:  previewBrandReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// ResolveProducts returns several products at once.
func (s *ProductQueryServer) ResolveProducts(ctx context.Context, req *catalogv1.ResolveProductsRequest) (*catalogv1.ResolveProductsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ResolveProducts", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "resolve products", "tenant", req.GetContext().GetTenantId())
	return &catalogv1.ResolveProductsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ExportProducts streams every matching product.
func (s *ProductQueryServer) ExportProducts(req *catalogv1.ExportProductsRequest, stream grpc.ServerStreamingServer[catalogv1.ExportProductsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		return err
	}
	s.log.InfoContext(ctx, "export products", "tenant", req.GetContext().GetTenantId())
	simulateBackorderRequest := &ordersv1.SimulateBackorderRequest{
		Context: req.GetContext(),
		Details: &ordersv1.SimulateBackorderRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	simulateBackorderReply, err := grpcx.Call(ctx, s.backorderCalculationClient.SimulateBackorder, simulateBackorderRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.orders.v1.BackorderCalculationService/SimulateBackorder")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := productQueryServerNormalizeCategorySteps[mode]
	normalizeCategoryRequest := &catalogv1.NormalizeCategoryRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Manager: &identityv1.UserRef{
						Email:  simulateBackorderReply.GetBackorder().GetCustomer().GetEmail(),
						UserId: simulateBackorderReply.GetBackorder().GetAssignee().GetUserId(),
					},
				}},
			}},
		},
	}
	normalizeCategoryReply, err := run(s, ctx, normalizeCategoryRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.catalog.v1.CategoryValidationService/NormalizeCategory")
	}
	if err := stream.Send(&catalogv1.ExportProductsResponse{
		Product: &catalogv1.Product{
			Audit: &commonv1.AuditInfo{
				CreatedAt: simulateBackorderReply.GetBackorder().GetAudit().GetCreatedAt(),
			},
			Name: normalizeCategoryReply.GetCategory().GetName(),
			Owner: &identityv1.UserRef{
				DisplayName: normalizeCategoryReply.GetCategory().GetManager().GetDisplayName(),
			},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}); err != nil {
		return err
	}
	return nil
}

// SuggestProducts lists products a page at a time.
func (s *ProductQueryServer) SuggestProducts(ctx context.Context, req *catalogv1.SuggestProductsRequest) (*catalogv1.SuggestProductsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "suggest products", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "region", req.GetFilter().GetRegion())
	previewCategoryRequest := &catalogv1.PreviewCategoryRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Location: &commonv1.PostalAddress{Region: tenancy.Value(ctx, "region")},
				Children: []*catalogv1.Category{&catalogv1.Category{
					Manager: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
				}},
			}},
		},
	}
	send := s.previewCategorySender(previewCategoryRequest)
	previewCategoryReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryValidationService/PreviewCategory")
	}
	lintCategoriesRequest := &catalogv1.LintCategoriesRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Manager: &identityv1.UserRef{
						Email: previewCategoryReply.GetCategory().GetManager().GetEmail(),
					},
					Audit: &commonv1.AuditInfo{
						CreatedBy: previewCategoryReply.GetCategory().GetAudit().GetCreatedBy(),
					},
				}},
				Location: &commonv1.PostalAddress{
					Region: previewCategoryReply.GetCategory().GetLocation().GetRegion(),
				},
				Manager: &identityv1.UserRef{
					UserId: previewCategoryReply.GetCategory().GetManager().GetUserId(),
				},
			}},
		},
	}
	lintCategoriesReply, err := s.lintCategoriesFn(ctx, lintCategoriesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryValidationService/LintCategories")
	}
	_ = lintCategoriesReply
	return &catalogv1.SuggestProductsResponse{
		Products: []*catalogv1.Product{&catalogv1.Product{
			Audit: &commonv1.AuditInfo{
				Etag:      previewCategoryReply.GetCategory().GetAudit().GetEtag(),
				CreatedAt: previewCategoryReply.GetCategory().GetAudit().GetCreatedAt(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// productQueryServerSimulateCategorySteps are the ways to run
// acme.catalog.v1.CategoryCalculationService/SimulateCategory, by name.
var productQueryServerSimulateCategorySteps = map[string]func(*ProductQueryServer, context.Context, *catalogv1.SimulateCategoryRequest) (*catalogv1.SimulateCategoryResponse, error){
	"default": (*ProductQueryServer).simulateCategoryStep,
	"audited": (*ProductQueryServer).simulateCategoryAuditedStep,
}

func (s *ProductQueryServer) simulateCategoryStep(ctx context.Context, request *catalogv1.SimulateCategoryRequest) (*catalogv1.SimulateCategoryResponse, error) {
	return s.categoryCalculationClient.SimulateCategory(ctx, request)
}

func (s *ProductQueryServer) simulateCategoryAuditedStep(ctx context.Context, request *catalogv1.SimulateCategoryRequest) (*catalogv1.SimulateCategoryResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.catalog.v1.CategoryCalculationService/SimulateCategory")
	return s.categoryCalculationClient.SimulateCategory(ctx, request)
}

// productQueryServerNormalizeCategorySteps are the ways to run
// acme.catalog.v1.CategoryValidationService/NormalizeCategory, by name.
var productQueryServerNormalizeCategorySteps = map[string]func(*ProductQueryServer, context.Context, *catalogv1.NormalizeCategoryRequest) (*catalogv1.NormalizeCategoryResponse, error){
	"default": (*ProductQueryServer).normalizeCategoryStep,
	"audited": (*ProductQueryServer).normalizeCategoryAuditedStep,
}

func (s *ProductQueryServer) normalizeCategoryStep(ctx context.Context, request *catalogv1.NormalizeCategoryRequest) (*catalogv1.NormalizeCategoryResponse, error) {
	return s.categoryValidationClient.NormalizeCategory(ctx, request)
}

func (s *ProductQueryServer) normalizeCategoryAuditedStep(ctx context.Context, request *catalogv1.NormalizeCategoryRequest) (*catalogv1.NormalizeCategoryResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.catalog.v1.CategoryValidationService/NormalizeCategory")
	return s.categoryValidationClient.NormalizeCategory(ctx, request)
}

// previewCategorySender returns a function that sends one request to
// acme.catalog.v1.CategoryValidationService/PreviewCategory.
func (s *ProductQueryServer) previewCategorySender(request *catalogv1.PreviewCategoryRequest) func(context.Context) (*catalogv1.PreviewCategoryResponse, error) {
	return func(ctx context.Context) (*catalogv1.PreviewCategoryResponse, error) {
		return s.categoryValidationClient.PreviewCategory(ctx, request)
	}
}
