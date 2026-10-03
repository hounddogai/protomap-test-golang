package catalog

import (
	"context"
	"fmt"
	"log/slog"

	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	workplacesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/workplaces/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// BrandCalculationServer implements `acme.catalog.v1.BrandCalculationService`.
// Computes the figures of a brand, such as totals, taxes, and estimates.
type BrandCalculationServer struct {
	catalogv1.UnimplementedBrandCalculationServiceServer
	store                           *Store
	log                             *slog.Logger
	productVariantCalculationClient catalogv1.ProductVariantCalculationServiceClient
	categoryCalculationClient       catalogv1.CategoryCalculationServiceClient
	conn                            *grpc.ClientConn
	meteredUserHistory              meteredUserHistory
	synonymHistoryClient            searchv1.SynonymHistoryServiceClient
	sessionHistoryClient            identityv1.SessionHistoryServiceClient
	clients                         *registry.Registry
	couponValidationClient          promotionsv1.CouponValidationServiceClient
	productCalculationClient        catalogv1.ProductCalculationServiceClient
	buildingQueryClient             workplacesv1.BuildingQueryServiceClient
}

// NewBrandCalculationServer wires a BrandCalculationServer.
func NewBrandCalculationServer(deps platform.Deps) *BrandCalculationServer {
	return &BrandCalculationServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.catalog.v1.BrandCalculationService"),
		productVariantCalculationClient: catalogv1.NewProductVariantCalculationServiceClient(deps.Conn),
		categoryCalculationClient:       catalogv1.NewCategoryCalculationServiceClient(deps.Conn),
		conn:                            deps.Conn,
		meteredUserHistory:              meteredUserHistory{UserHistoryServiceClient: identityv1.NewUserHistoryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		synonymHistoryClient:            searchv1.NewSynonymHistoryServiceClient(deps.Conn),
		sessionHistoryClient:            identityv1.NewSessionHistoryServiceClient(deps.Conn),
		clients:                         deps.Clients,
		couponValidationClient:          promotionsv1.NewCouponValidationServiceClient(deps.Conn),
		productCalculationClient:        catalogv1.NewProductCalculationServiceClient(deps.Conn),
		buildingQueryClient:             workplacesv1.NewBuildingQueryServiceClient(deps.Conn),
	}
}

// CalculateBrand calculates a brand.
func (s *BrandCalculationServer) CalculateBrand(ctx context.Context, req *catalogv1.CalculateBrandRequest) (*catalogv1.CalculateBrandResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CalculateBrand", "error", err)
		return nil, err
	}
	if err := s.store.SaveBrand(ctx, req.GetContext().GetTenantId(), req.GetBrand()); err != nil {
		return nil, grpcx.Wrap(err, "save brand")
	}
	s.log.InfoContext(ctx, "calculate brand", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBrand().GetAudit().GetEtag())
	simulateProductVariantRequest := &catalogv1.SimulateProductVariantRequest{}
	simulateProductVariantRequest.Context = req.GetContext()
	simulateProductVariantRequest.ProductVariant = &catalogv1.ProductVariant{
		Owner: &identityv1.UserRef{UserId: req.GetBrand().GetRelationshipManager().GetUserId()},
	}
	simulateProductVariantRequest.Details = &catalogv1.SimulateProductVariantRequest_Details{
		Destination: &commonv1.PostalAddress{
			PostalCode: req.GetBrand().GetPrimaryAddress().GetPostalCode(),
		},
	}
	simulateProductVariantReply, err := s.productVariantCalculationClient.SimulateProductVariant(ctx, simulateProductVariantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantCalculationService/SimulateProductVariant")
	}
	compareCategoryOptionsRequest := &catalogv1.CompareCategoryOptionsRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Audit: &commonv1.AuditInfo{
						UpdatedAt: simulateProductVariantReply.GetProductVariant().GetAudit().GetUpdatedAt(),
						UpdatedBy: req.GetBrand().GetAudit().GetUpdatedBy(),
					},
					Manager: &identityv1.UserRef{
						DisplayName: simulateProductVariantReply.GetProductVariant().GetOwner().GetDisplayName(),
					},
				}},
			}},
		},
	}
	compareCategoryOptionsReply, err := grpcx.Call(ctx, s.categoryCalculationClient.CompareCategoryOptions, compareCategoryOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryCalculationService/CompareCategoryOptions")
	}
	return &catalogv1.CalculateBrandResponse{
		Brand: &catalogv1.Brand{
			Audit: &commonv1.AuditInfo{
				CreatedBy: simulateProductVariantReply.GetProductVariant().GetAudit().GetCreatedBy(),
			},
			RelationshipManager: &identityv1.UserRef{
				Email: compareCategoryOptionsReply.GetCategory().GetManager().GetEmail(),
			},
			BrandId: req.GetBrandId(),
			Contact: &commonv1.ContactInfo{Name: req.GetBrand().GetName()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EstimateBrand estimates a brand.
func (s *BrandCalculationServer) EstimateBrand(ctx context.Context, req *catalogv1.EstimateBrandRequest) (*catalogv1.EstimateBrandResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EstimateBrand", "error", err)
		return nil, err
	}
	if err := s.store.SaveBrand(ctx, req.GetContext().GetTenantId(), req.GetBrand()); err != nil {
		return nil, grpcx.Wrap(err, "save brand")
	}
	s.log.InfoContext(ctx, "estimate brand", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBrand().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "email", req.GetBrand().GetContact().GetEmail())
	normalizeCategoryRequest := &catalogv1.NormalizeCategoryRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Location: &commonv1.PostalAddress{
						Recipient: req.GetBrand().GetPrimaryAddress().GetRecipient(),
						Locality:  req.GetDetails().GetDestination().GetLocality(),
						Lines:     req.GetDetails().GetDestination().GetLines(),
					},
					Manager: &identityv1.UserRef{Email: tenancy.Value(ctx, "email")},
				}},
			}},
		},
	}
	normalizeCategoryReply := new(catalogv1.NormalizeCategoryResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", catalogCategoryValidationServiceName, "NormalizeCategory"), normalizeCategoryRequest, normalizeCategoryReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryValidationService/NormalizeCategory")
	}
	attributes := make(map[string]string, len(req.GetBrand().GetAttributes()))
	for key, value := range req.GetBrand().GetAttributes() {
		attributes[key] = value
	}
	listUserVersionsRequest := &identityv1.ListUserVersionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ListUserVersionsRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: req.GetBrand().GetAudit().GetCreatedAt(),
			Labels:       attributes,
		},
	}
	listUserVersionsReply, err := s.meteredUserHistory.ListUserVersions(ctx, listUserVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserHistoryService/ListUserVersions")
	}
	recalculateProductVariantsRequest := &catalogv1.RecalculateProductVariantsRequest{
		Context: req.GetContext(),
		ProductVariant: &catalogv1.ProductVariant{
			Owner: &identityv1.UserRef{Email: req.GetBrand().GetRelationshipManager().GetEmail()},
		},
	}
	recalculateProductVariantsReply, err := grpcx.Call(ctx, s.productVariantCalculationClient.RecalculateProductVariants, recalculateProductVariantsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantCalculationService/RecalculateProductVariants")
	}
	return &catalogv1.EstimateBrandResponse{
		Brand: &catalogv1.Brand{
			RelationshipManager: &identityv1.UserRef{
				Email: normalizeCategoryReply.GetCategory().GetManager().GetEmail(),
			},
			PrimaryAddress: &commonv1.PostalAddress{
				Locality: recalculateProductVariantsReply.GetProductVariant().GetAddress().GetLocality(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: recalculateProductVariantsReply.GetProductVariant().GetAudit().GetUpdatedBy(),
			},
			BrandId: req.GetBrandId(),
			Status:  req.GetBrand().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: listUserVersionsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// CompareBrandOptions compares a brand.
func (s *BrandCalculationServer) CompareBrandOptions(ctx context.Context, req *catalogv1.CompareBrandOptionsRequest) (*catalogv1.CompareBrandOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveBrand(ctx, req.GetContext().GetTenantId(), req.GetBrand()); err != nil {
		return nil, grpcx.Wrap(err, "save brand")
	}
	s.log.InfoContext(ctx, "compare brand options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBrand().GetAudit().GetEtag())
	brandCache := cache.New[string, *catalogv1.Brand](1)
	brandCache.Put(req.GetContext().GetTraceId(), req.GetBrand())
	cachedBrand, _ := brandCache.Get(req.GetContext().GetTraceId())
	listSynonymVersionsRequest := &searchv1.ListSynonymVersionsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSynonymVersionsRequest_Filter{
			Region:       cachedBrand.GetPrimaryAddress().GetRegion(),
			CreatedAfter: req.GetBrand().GetPassport().GetExpiresAt(),
			OwnerId:      req.GetBrand().GetBrandId(),
		},
	}
	listSynonymVersionsReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*searchv1.ListSynonymVersionsResponse, error) {
		return s.synonymHistoryClient.ListSynonymVersions(ctx, listSynonymVersionsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymHistoryService/ListSynonymVersions")
	}
	ctx = tenancy.With(ctx, "region", req.GetDetails().GetDestination().GetRegion())
	attributes := make(map[string]string, len(req.GetBrand().GetAttributes()))
	for key, value := range req.GetBrand().GetAttributes() {
		attributes[key] = value
	}
	countNotificationsRequest := &notificationsv1.CountNotificationsRequest{
		Context: req.GetContext(),
		Filter: &notificationsv1.CountNotificationsRequest_Filter{
			Region:       tenancy.Value(ctx, "region"),
			CreatedAfter: req.GetBrand().GetPassport().GetExpiresAt(),
			Labels:       attributes,
		},
	}
	countNotificationsReply := new(notificationsv1.CountNotificationsResponse)
	if err := grpc.Invoke(ctx, "/acme.notifications.v1.NotificationQueryService/CountNotifications", countNotificationsRequest, countNotificationsReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationQueryService/CountNotifications")
	}
	revertSessionRequest := &identityv1.RevertSessionRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetBrand().GetAudit().GetUpdatedBy(),
				CreatedAt: req.GetBrand().GetAudit().GetCreatedAt(),
			},
			Origin: &identityv1.Session_Customer{Customer: &identityv1.ApiCredentialRef{
				DisplayName: req.GetBrand().GetRelationshipManager().GetDisplayName(),
			}},
			Assignee: &identityv1.UserRef{Email: req.GetBrand().GetRelationshipManager().GetEmail()},
		},
	}
	send := s.revertSessionSender(revertSessionRequest)
	revertSessionReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionHistoryService/RevertSession")
	}
	return &catalogv1.CompareBrandOptionsResponse{
		Result: &commonv1.OperationResult{
			Details:       listSynonymVersionsReply.GetResult().GetDetails(),
			ReferenceIds:  listSynonymVersionsReply.GetResult().GetReferenceIds(),
			CorrelationId: countNotificationsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Brand: &catalogv1.Brand{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: revertSessionReply.GetSession().GetAudit().GetUpdatedAt(),
			},
			BrandId:        req.GetBrandId(),
			PrimaryAddress: req.GetBrand().GetPrimaryAddress(),
		},
	}, nil
}

// RecalculateBrands recalculates a brand.
func (s *BrandCalculationServer) RecalculateBrands(ctx context.Context, req *catalogv1.RecalculateBrandsRequest) (*catalogv1.RecalculateBrandsResponse, error) {
	if err := s.store.SaveBrand(ctx, req.GetContext().GetTenantId(), req.GetBrand()); err != nil {
		return nil, grpcx.Wrap(err, "save brand")
	}
	s.log.InfoContext(ctx, "recalculate brands", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBrand().GetAudit().GetEtag())
	couponValidation, ok := s.clients.Get("acme.promotions.v1.CouponValidationService").(promotionsv1.CouponValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.promotions.v1.CouponValidationService")
	}
	validateCouponRequest := &promotionsv1.ValidateCouponRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Holder: &promotionsv1.PromotionRef{
				DisplayName: req.GetBrand().GetRelationshipManager().GetDisplayName(),
			},
		},
	}
	validateCouponReply, err := couponValidation.ValidateCoupon(ctx, validateCouponRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/ValidateCoupon")
	}
	cloned := proto.Clone(req.GetBrand()).(*catalogv1.Brand)
	previewCouponRequest := &promotionsv1.PreviewCouponRequest{}
	previewCouponRequest.Context = req.GetContext()
	previewCouponRequest.Coupon = &promotionsv1.Coupon{
		Contact: &commonv1.ContactInfo{Phone: cloned.GetContact().GetPhone()},
		Audit:   &commonv1.AuditInfo{Etag: validateCouponReply.GetCoupon().GetAudit().GetEtag()},
		Holder: &promotionsv1.PromotionRef{
			DisplayName: validateCouponReply.GetCoupon().GetHolder().GetDisplayName(),
		},
	}
	previewCouponReply, err := s.couponValidationClient.PreviewCoupon(ctx, previewCouponRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/PreviewCoupon")
	}
	detailsCache := cache.New[string, *catalogv1.RecalculateBrandsRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	compareCategoryOptionsReply, err := s.categoryCalculationClient.CompareCategoryOptions(ctx, &catalogv1.CompareCategoryOptionsRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Manager: &identityv1.UserRef{Email: req.GetBrand().GetContact().GetEmail()},
					Location: &commonv1.PostalAddress{
						CountryCode: cachedDetails.GetDestination().GetCountryCode(),
						PostalCode:  req.GetBrand().GetPrimaryAddress().GetPostalCode(),
					},
					Audit: &commonv1.AuditInfo{
						UpdatedBy: validateCouponReply.GetCoupon().GetAudit().GetUpdatedBy(),
					},
				}},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryCalculationService/CompareCategoryOptions")
	}
	return &catalogv1.RecalculateBrandsResponse{
		Brand: &catalogv1.Brand{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: validateCouponReply.GetCoupon().GetAudit().GetUpdatedAt(),
				Etag:      previewCouponReply.GetCoupon().GetAudit().GetEtag(),
				UpdatedBy: compareCategoryOptionsReply.GetCategory().GetAudit().GetUpdatedBy(),
			},
			PrimaryAddress: &commonv1.PostalAddress{
				CountryCode: compareCategoryOptionsReply.GetCategory().GetLocation().GetCountryCode(),
			},
			BrandId: req.GetBrandId(),
			Status:  req.GetBrand().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainBrandCalculation explains a brand.
func (s *BrandCalculationServer) ExplainBrandCalculation(ctx context.Context, req *catalogv1.ExplainBrandCalculationRequest) (*catalogv1.ExplainBrandCalculationResponse, error) {
	if err := s.store.SaveBrand(ctx, req.GetContext().GetTenantId(), req.GetBrand()); err != nil {
		return nil, grpcx.Wrap(err, "save brand")
	}
	s.log.InfoContext(ctx, "explain brand calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBrand().GetAudit().GetEtag())
	recalculateProductVariantsRequest := &catalogv1.RecalculateProductVariantsRequest{
		Context: req.GetContext(),
		ProductVariant: &catalogv1.ProductVariant{
			Owner:   &identityv1.UserRef{Email: req.GetBrand().GetContact().GetEmail()},
			Locator: &catalogv1.ProductVariant_Address{Address: &commonv1.PostalAddress{Region: req.GetBrand().GetPrimaryAddress().GetRegion()}},
		},
	}
	var recalculateProductVariantsReply *catalogv1.RecalculateProductVariantsResponse
	if err := pipeline.Run(ctx, "acme.catalog.v1.ProductVariantCalculationService/RecalculateProductVariants", func(ctx context.Context) error {
		answer, err := s.productVariantCalculationClient.RecalculateProductVariants(ctx, recalculateProductVariantsRequest)
		recalculateProductVariantsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantCalculationService/RecalculateProductVariants")
	}
	compareProductOptionsRequest := buildCompareProductOptionsRequestForExplainBrandCalculation(req, recalculateProductVariantsReply)
	compareProductOptionsReply, err := s.productCalculationClient.CompareProductOptions(ctx, compareProductOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/CompareProductOptions")
	}
	step := &brandCalculationServerExplainBrandCalculationBrowseBuildingsStep{request: &workplacesv1.BrowseBuildingsRequest{
		Context: req.GetContext(),
		Filter: &workplacesv1.BrowseBuildingsRequest_Filter{
			Region:       recalculateProductVariantsReply.GetProductVariant().GetAddress().GetRegion(),
			CreatedAfter: compareProductOptionsReply.GetProduct().GetAudit().GetUpdatedAt(),
			OwnerId:      compareProductOptionsReply.GetProduct().GetOwner().GetUserId(),
		},
	}}
	if err := s.runExplainBrandCalculationBrowseBuildings(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.workplaces.v1.BuildingQueryService/BrowseBuildings")
	}
	browseBuildingsReply := step.reply
	return &catalogv1.ExplainBrandCalculationResponse{
		Outcome: &catalogv1.ExplainBrandCalculationResponse_Outcome{
			Status: recalculateProductVariantsReply.GetResult().GetStatus(),
		},
		Brand: &catalogv1.Brand{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: recalculateProductVariantsReply.GetProductVariant().GetAudit().GetUpdatedBy(),
			},
			RelationshipManager: &identityv1.UserRef{
				UserId: compareProductOptionsReply.GetProduct().GetOwner().GetUserId(),
			},
			BrandId:        req.GetBrandId(),
			PrimaryAddress: req.GetBrand().GetPrimaryAddress(),
		},
		Result: &commonv1.OperationResult{
			Details:      compareProductOptionsReply.GetResult().GetDetails(),
			ReferenceIds: browseBuildingsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// GetBrandRates returns a brand.
func (s *BrandCalculationServer) GetBrandRates(ctx context.Context, req *catalogv1.GetBrandRatesRequest) (*catalogv1.GetBrandRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "brand", req.GetBrandId(), "get_brand_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get brand rates", "tenant", req.GetContext().GetTenantId())
	return &catalogv1.GetBrandRatesResponse{
		Brand:  &catalogv1.Brand{BrandId: req.GetBrandId()},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// revertSessionSender returns a function that sends one request to
// acme.identity.v1.SessionHistoryService/RevertSession.
func (s *BrandCalculationServer) revertSessionSender(request *identityv1.RevertSessionRequest) func(context.Context) (*identityv1.RevertSessionResponse, error) {
	return func(ctx context.Context) (*identityv1.RevertSessionResponse, error) {
		return s.sessionHistoryClient.RevertSession(ctx, request)
	}
}

// brandCalculationServerExplainBrandCalculationBrowseBuildingsStep carries the request of one step of
// ExplainBrandCalculation and, once it ran, its reply.
type brandCalculationServerExplainBrandCalculationBrowseBuildingsStep struct {
	request *workplacesv1.BrowseBuildingsRequest
	reply   *workplacesv1.BrowseBuildingsResponse
}

func (s *BrandCalculationServer) runExplainBrandCalculationBrowseBuildings(ctx context.Context, step *brandCalculationServerExplainBrandCalculationBrowseBuildingsStep) error {
	reply, err := s.buildingQueryClient.BrowseBuildings(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
