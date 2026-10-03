package catalog

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	showsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shows/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ProductServer implements `acme.catalog.v1.ProductService`.
// Manages products: creates, reads, changes, and lists them.
type ProductServer struct {
	catalogv1.UnimplementedProductServiceServer
	store                             *Store
	log                               *slog.Logger
	clients                           *registry.Registry
	priceListQueryClient              pricingv1.PriceListQueryServiceClient
	bus                               *eventbus.Bus
	conn                              *grpc.ClientConn
	brandCalculationOnce              sync.Once
	brandCalculationCached            catalogv1.BrandCalculationServiceClient
	attributeValidationClient         catalogv1.AttributeValidationServiceClient
	attributeValidationClientFallback catalogv1.AttributeValidationServiceClient
	cartQueryClient                   cartv1.CartQueryServiceClient
	meteredFulfillment                meteredFulfillment
	synonymExportClient               searchv1.SynonymExportServiceClient
	notificationQueryClient           notificationsv1.NotificationQueryServiceClient
	attributeCalculationClient        catalogv1.AttributeCalculationServiceClient
	productVariantCalculationClient   catalogv1.ProductVariantCalculationServiceClient
}

// NewProductServer wires a ProductServer.
func NewProductServer(deps platform.Deps) *ProductServer {
	return &ProductServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.catalog.v1.ProductService"),
		clients:                           deps.Clients,
		priceListQueryClient:              pricingv1.NewPriceListQueryServiceClient(deps.Conn),
		bus:                               deps.Bus,
		conn:                              deps.Conn,
		attributeValidationClient:         catalogv1.NewAttributeValidationServiceClient(deps.Conn),
		attributeValidationClientFallback: catalogv1.NewAttributeValidationServiceClient(deps.Conn),
		cartQueryClient:                   cartv1.NewCartQueryServiceClient(deps.Conn),
		meteredFulfillment:                meteredFulfillment{FulfillmentServiceClient: fulfillmentv1.NewFulfillmentServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		synonymExportClient:               searchv1.NewSynonymExportServiceClient(deps.Conn),
		notificationQueryClient:           notificationsv1.NewNotificationQueryServiceClient(deps.Conn),
		attributeCalculationClient:        catalogv1.NewAttributeCalculationServiceClient(deps.Conn),
		productVariantCalculationClient:   catalogv1.NewProductVariantCalculationServiceClient(deps.Conn),
	}
}

// CreateProduct creates a product.
func (s *ProductServer) CreateProduct(ctx context.Context, req *catalogv1.CreateProductRequest) (*catalogv1.CreateProductResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CreateProduct", "error", err)
		return nil, err
	}
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "create product", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	showCalculation, ok := s.clients.Get("acme.shows.v1.ShowCalculationService").(showsv1.ShowCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.shows.v1.ShowCalculationService")
	}
	var postalCode string
	switch choice := req.GetProduct().GetLocator().(type) {
	case *catalogv1.Product_Address:
		postalCode = choice.Address.GetPostalCode()
	case *catalogv1.Product_FacilityCode:
		postalCode = choice.FacilityCode
	}
	estimateShowRequest := &showsv1.EstimateShowRequest{
		Context: req.GetContext(),
		Show: &showsv1.Show{
			Location:  &commonv1.PostalAddress{PostalCode: postalCode},
			Organizer: &identityv1.UserRef{DisplayName: req.GetProduct().GetOwner().GetDisplayName()},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetProduct().GetAudit().GetUpdatedBy(),
				UpdatedAt: req.GetProduct().GetAudit().GetUpdatedAt(),
			},
		},
	}
	estimateShowReply, err := showCalculation.EstimateShow(ctx, estimateShowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shows.v1.ShowCalculationService/EstimateShow")
	}
	browsePriceListsRequest := &pricingv1.BrowsePriceListsRequest{}
	browsePriceListsRequest.Context = req.GetContext()
	browsePriceListsRequest.Filter = &pricingv1.BrowsePriceListsRequest_Filter{
		Region:       estimateShowReply.GetShow().GetLocation().GetRegion(),
		CreatedAfter: req.GetProduct().GetAudit().GetUpdatedAt(),
		OwnerId:      estimateShowReply.GetShow().GetShowId(),
		Labels:       estimateShowReply.GetResult().GetDetails(),
	}
	browsePriceListsReply, err := s.priceListQueryClient.BrowsePriceLists(ctx, browsePriceListsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListQueryService/BrowsePriceLists")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "catalog.create_product.validate_product_variant",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"product_variant.owner.email":   req.GetProduct().GetOwner().GetEmail(),
			"product_variant.owner.user_id": req.GetProduct().GetOwner().GetUserId(),
			"context.tenant_id":             req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &catalogv1.CreateProductResponse{
		Product: &catalogv1.Product{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: estimateShowReply.GetShow().GetAudit().GetUpdatedBy(),
			},
			Owner: req.GetProduct().GetOwner(),
			Specs: req.GetProduct().GetSpecs(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: estimateShowReply.GetResult().GetReferenceIds(),
			Details:      browsePriceListsReply.GetResult().GetDetails(),
			Status:       "ok",
		},
	}, nil
}

// GetProduct returns a product.
func (s *ProductServer) GetProduct(ctx context.Context, req *catalogv1.GetProductRequest) (*catalogv1.GetProductResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product", req.GetProductId(), "get_product"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get product", "tenant", req.GetContext().GetTenantId())
	var cachedCode string
	if entry, ok := recentProducts.Load(recordKey{req.GetContext().GetTenantId(), req.GetProductId()}); ok {
		if previous, ok := entry.(*catalogv1.Product); ok {
			cachedCode = previous.GetCode()
		}
	}
	recalculateBrandsRequest := &catalogv1.RecalculateBrandsRequest{
		Context: req.GetContext(),
		Brand: &catalogv1.Brand{
			RelationshipManager: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
		Reason: cachedCode,
	}
	recalculateBrandsReply, err := s.brandCalculationLazy().RecalculateBrands(ctx, recalculateBrandsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandCalculationService/RecalculateBrands")
	}
	return &catalogv1.GetProductResponse{
		Product: &catalogv1.Product{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: recalculateBrandsReply.GetBrand().GetAudit().GetUpdatedAt(),
			},
			ProductId: req.GetProductId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// UpdateProduct changes a product.
func (s *ProductServer) UpdateProduct(ctx context.Context, req *catalogv1.UpdateProductRequest) (*catalogv1.UpdateProductResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected UpdateProduct", "error", err)
		return nil, err
	}
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "update product", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *catalogv1.PreviewAttributeRequest, ...grpc.CallOption) (*catalogv1.PreviewAttributeResponse, error){
		"primary":  s.attributeValidationClient.PreviewAttribute,
		"fallback": s.attributeValidationClientFallback.PreviewAttribute,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	ctx = tenancy.With(ctx, "description", req.GetProduct().GetComponents()["primary"].GetDescription())
	previewAttributeRequest := &catalogv1.PreviewAttributeRequest{
		Context: req.GetContext(),
		Attribute: &catalogv1.Attribute{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetProduct().GetAudit().GetEtag(),
				CreatedBy: req.GetProduct().GetAudit().GetCreatedBy(),
			},
			Owner:       &identityv1.UserRef{Email: req.GetProduct().GetOwner().GetEmail()},
			Description: tenancy.Value(ctx, "description"),
		},
	}
	previewAttributeReply, err := route(ctx, previewAttributeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.AttributeValidationService/PreviewAttribute")
	}
	exportCartsStream, err := s.cartQueryClient.ExportCarts(ctx, &cartv1.ExportCartsRequest{
		Context: req.GetContext(),
		Filter: &cartv1.ExportCartsRequest_Filter{
			Region:       req.GetProduct().GetAddress().GetRegion(),
			CreatedAfter: req.GetProduct().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetProduct().GetComponents()["primary"].GetComponentId(),
			Labels:       previewAttributeReply.GetResult().GetDetails(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartQueryService/ExportCarts")
	}
	for {
		received, err := exportCartsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.cart.v1.CartQueryService/ExportCarts")
		}
		_ = received
	}
	recentProducts.Store(recordKey{req.GetContext().GetTenantId(), req.GetProduct().GetProductId()}, req.GetProduct())
	return &catalogv1.UpdateProductResponse{
		Product: &catalogv1.Product{
			Owner: &identityv1.UserRef{
				UserId:      previewAttributeReply.GetAttribute().GetOwner().GetUserId(),
				DisplayName: previewAttributeReply.GetAttribute().GetOwner().GetDisplayName(),
			},
			Name: req.GetProduct().GetName(),
			Code: req.GetProduct().GetCode(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// DeleteProduct deletes a product.
func (s *ProductServer) DeleteProduct(ctx context.Context, req *catalogv1.DeleteProductRequest) (*catalogv1.DeleteProductResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product", req.GetProductId(), "delete_product"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete product", "tenant", req.GetContext().GetTenantId())
	batchGetFulfillmentsRequest := &fulfillmentv1.BatchGetFulfillmentsRequest{Context: req.GetContext()}
	batchGetFulfillmentsReply, err := s.meteredFulfillment.BatchGetFulfillments(ctx, batchGetFulfillmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentService/BatchGetFulfillments")
	}
	retrySynonymExportRequest := &searchv1.RetrySynonymExportRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Action: &searchv1.Synonym_Block{Block: &searchv1.SynonymBlockAction{Reason: req.GetReason()}},
			Audit:  &commonv1.AuditInfo{Etag: req.GetEtag()},
		},
		Details: &searchv1.RetrySynonymExportRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	send := s.retrySynonymExportSender(retrySynonymExportRequest)
	retrySynonymExportReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/RetrySynonymExport")
	}
	productVariantValidation, ok := s.clients.Get("acme.catalog.v1.ProductVariantValidationService").(catalogv1.ProductVariantValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.catalog.v1.ProductVariantValidationService")
	}
	explainProductVariantDecisionRequest := &catalogv1.ExplainProductVariantDecisionRequest{
		Context: req.GetContext(),
		ProductVariant: &catalogv1.ProductVariant{
			Audit: &commonv1.AuditInfo{
				Etag:      retrySynonymExportReply.GetSynonym().GetAudit().GetEtag(),
				CreatedAt: retrySynonymExportReply.GetSynonym().GetAudit().GetCreatedAt(),
				CreatedBy: retrySynonymExportReply.GetSynonym().GetAudit().GetCreatedBy(),
			},
		},
	}
	explainProductVariantDecisionReply, err := productVariantValidation.ExplainProductVariantDecision(ctx, explainProductVariantDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantValidationService/ExplainProductVariantDecision")
	}
	return &catalogv1.DeleteProductResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: batchGetFulfillmentsReply.GetResult().GetCorrelationId(),
			Details:       retrySynonymExportReply.GetResult().GetDetails(),
			Status:        explainProductVariantDecisionReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// BatchGetProducts returns several products at once.
func (s *ProductServer) BatchGetProducts(ctx context.Context, req *catalogv1.BatchGetProductsRequest) (*catalogv1.BatchGetProductsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected BatchGetProducts", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "batch get products", "tenant", req.GetContext().GetTenantId())
	return &catalogv1.BatchGetProductsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// BatchCreateProducts takes a stream of products and answers once at the end.
func (s *ProductServer) BatchCreateProducts(stream grpc.ClientStreamingServer[catalogv1.BatchCreateProductsRequest, catalogv1.BatchCreateProductsResponse]) error {
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
		s.log.InfoContext(ctx, "batch create products", "tenant", msg.GetContext().GetTenantId())
		exportNotificationsStream, err := s.notificationQueryClient.ExportNotifications(ctx, &notificationsv1.ExportNotificationsRequest{
			Context: msg.GetContext(),
			Filter: &notificationsv1.ExportNotificationsRequest_Filter{
				Region:       msg.GetRecord().GetAddress().GetRegion(),
				CreatedAfter: msg.GetRecord().GetAudit().GetUpdatedAt(),
			},
			Format: msg.GetHeader().GetFormat(),
		})
		if err != nil {
			return grpcx.Wrap(err, "acme.notifications.v1.NotificationQueryService/ExportNotifications")
		}
		for {
			received, err := exportNotificationsStream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return grpcx.Wrap(err, "acme.notifications.v1.NotificationQueryService/ExportNotifications")
			}
			_ = received
		}
		if _, err := s.attributeCalculationClient.ExplainAttributeCalculation(ctx, &catalogv1.ExplainAttributeCalculationRequest{
			Context: msg.GetContext(),
			Attribute: &catalogv1.Attribute{
				Audit: &commonv1.AuditInfo{
					UpdatedAt: msg.GetRecord().GetAudit().GetUpdatedAt(),
					UpdatedBy: msg.GetRecord().GetAudit().GetUpdatedBy(),
				},
			},
		}); err != nil {
			return grpcx.Wrap(err, "acme.catalog.v1.AttributeCalculationService/ExplainAttributeCalculation")
		}
		accepted++
	}
	return stream.SendAndClose(&catalogv1.BatchCreateProductsResponse{Accepted: accepted})
}

// CloneProduct creates a product.
func (s *ProductServer) CloneProduct(ctx context.Context, req *catalogv1.CloneProductRequest) (*catalogv1.CloneProductResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CloneProduct", "error", err)
		return nil, err
	}
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "clone product", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	recalculateManifestsRequest := &shippingv1.RecalculateManifestsRequest{
		Context: req.GetContext(),
		Manifest: &shippingv1.Manifest{
			Counterparty: &customersv1.CustomerRef{Email: req.GetProduct().GetOwner().GetEmail()},
		},
	}
	recalculateManifestsReply := new(shippingv1.RecalculateManifestsResponse)
	if err := grpcx.Invoke(ctx, s.conn, shippingv1.ManifestCalculationService_RecalculateManifests_FullMethodName, recalculateManifestsRequest, recalculateManifestsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestCalculationService/RecalculateManifests")
	}
	compareProductVariantOptionsReply, err := s.productVariantCalculationClient.CompareProductVariantOptions(ctx, &catalogv1.CompareProductVariantOptionsRequest{
		Context: req.GetContext(),
		Details: &catalogv1.CompareProductVariantOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: recalculateManifestsReply.GetManifest().GetBillingAddress().GetPostalCode(),
			},
		},
		ProductVariant: &catalogv1.ProductVariant{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetProduct().GetAudit().GetCreatedAt()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantCalculationService/CompareProductVariantOptions")
	}
	recentProducts.Store(recordKey{req.GetContext().GetTenantId(), req.GetProduct().GetProductId()}, req.GetProduct())
	return &catalogv1.CloneProductResponse{
		Product: &catalogv1.Product{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: recalculateManifestsReply.GetManifest().GetAudit().GetUpdatedBy(),
			},
			Capacity: req.GetProduct().GetCapacity(),
			Owner:    req.GetProduct().GetOwner(),
		},
		Result: &commonv1.OperationResult{
			Message: compareProductVariantOptionsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// brandCalculationLazy dials acme.catalog.v1.BrandCalculationService the first time a handler needs it.
func (s *ProductServer) brandCalculationLazy() catalogv1.BrandCalculationServiceClient {
	s.brandCalculationOnce.Do(func() { s.brandCalculationCached = catalogv1.NewBrandCalculationServiceClient(s.conn) })
	return s.brandCalculationCached
}

// retrySynonymExportSender returns a function that sends one request to
// acme.search.v1.SynonymExportService/RetrySynonymExport.
func (s *ProductServer) retrySynonymExportSender(request *searchv1.RetrySynonymExportRequest) func(context.Context) (*searchv1.RetrySynonymExportResponse, error) {
	return func(ctx context.Context) (*searchv1.RetrySynonymExportResponse, error) {
		return s.synonymExportClient.RetrySynonymExport(ctx, request)
	}
}
