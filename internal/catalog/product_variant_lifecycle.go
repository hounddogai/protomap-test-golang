package catalog

import (
	"context"
	"fmt"
	"log/slog"

	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// ProductVariantLifecycleServer implements `acme.catalog.v1.ProductVariantLifecycleService`.
// Moves a product variant between the states of its lifecycle.
type ProductVariantLifecycleServer struct {
	catalogv1.UnsafeProductVariantLifecycleServiceServer
	store                         *Store
	log                           *slog.Logger
	userClient                    identityv1.UserServiceClient
	clients                       *registry.Registry
	productVariantValidationPort  productVariantValidationPort
	attributeValidationClient     catalogv1.AttributeValidationServiceClient
	categoryValidationClient      catalogv1.CategoryValidationServiceClient
	conn                          *grpc.ClientConn
	categoryQueryClient           catalogv1.CategoryQueryServiceClient
	productVariantReportingClient catalogv1.ProductVariantReportingServiceClient
	meteredProductCalculation     meteredProductCalculation
}

// NewProductVariantLifecycleServer wires a ProductVariantLifecycleServer.
func NewProductVariantLifecycleServer(deps platform.Deps) *ProductVariantLifecycleServer {
	return &ProductVariantLifecycleServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.catalog.v1.ProductVariantLifecycleService"),
		userClient:                    identityv1.NewUserServiceClient(deps.Conn),
		clients:                       deps.Clients,
		productVariantValidationPort:  newProductVariantValidationPort(deps),
		attributeValidationClient:     catalogv1.NewAttributeValidationServiceClient(deps.Conn),
		categoryValidationClient:      catalogv1.NewCategoryValidationServiceClient(deps.Conn),
		conn:                          deps.Conn,
		categoryQueryClient:           catalogv1.NewCategoryQueryServiceClient(deps.Conn),
		productVariantReportingClient: catalogv1.NewProductVariantReportingServiceClient(deps.Conn),
		meteredProductCalculation:     meteredProductCalculation{ProductCalculationServiceClient: catalogv1.NewProductCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}
}

// ActivateProductVariant activates a product variant.
func (s *ProductVariantLifecycleServer) ActivateProductVariant(ctx context.Context, req *catalogv1.ActivateProductVariantRequest) (*catalogv1.ActivateProductVariantResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ActivateProductVariant", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product_variant", req.GetProductVariantId(), "activate_product_variant"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate product variant", "tenant", req.GetContext().GetTenantId())
	listUsersRequest := buildListUsersRequestForActivateProductVariant(req)
	listUsersReply, err := s.userClient.ListUsers(ctx, listUsersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/ListUsers")
	}
	return &catalogv1.ActivateProductVariantResponse{
		Result: &commonv1.OperationResult{
			Message:      listUsersReply.GetResult().GetMessage(),
			ReferenceIds: listUsersReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		ProductVariant: &catalogv1.ProductVariant{ProductVariantId: req.GetProductVariantId()},
		Outcome:        &catalogv1.ActivateProductVariantResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// SuspendProductVariant suspends a product variant.
func (s *ProductVariantLifecycleServer) SuspendProductVariant(ctx context.Context, req *catalogv1.SuspendProductVariantRequest) (*catalogv1.SuspendProductVariantResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product_variant", req.GetProductVariantId(), "suspend_product_variant"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend product variant", "tenant", req.GetContext().GetTenantId())
	attributeSync, err := registry.Lookup[catalogv1.AttributeSyncServiceClient](s.clients, "acme.catalog.v1.AttributeSyncService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pushAttributeRequest := &catalogv1.PushAttributeRequest{
		Context: req.GetContext(),
		Attribute: &catalogv1.Attribute{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetNewOwner().GetEmail()},
		},
		Details: &catalogv1.PushAttributeRequest_Details{Payload: snapshot},
	}
	pushAttributeReply, err := attributeSync.PushAttribute(ctx, pushAttributeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.AttributeSyncService/PushAttribute")
	}
	return &catalogv1.SuspendProductVariantResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: pushAttributeReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		ProductVariant: &catalogv1.ProductVariant{
			Components: map[string]*catalogv1.ProductVariantComponent{"primary": &catalogv1.ProductVariantComponent{
				Description: pushAttributeReply.GetAttribute().GetDescription(),
			}},
			ProductVariantId: req.GetProductVariantId(),
		},
		Outcome: &catalogv1.SuspendProductVariantResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ReinstateProductVariant reinstates a product variant.
func (s *ProductVariantLifecycleServer) ReinstateProductVariant(ctx context.Context, req *catalogv1.ReinstateProductVariantRequest) (*catalogv1.ReinstateProductVariantResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product_variant", req.GetProductVariantId(), "reinstate_product_variant"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate product variant", "tenant", req.GetContext().GetTenantId())
	previewProductVariantRequest := &catalogv1.PreviewProductVariantRequest{
		Context: req.GetContext(),
		ProductVariant: &catalogv1.ProductVariant{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
				UserId:      req.GetDetails().GetNewOwner().GetUserId(),
			},
		},
		Details: &catalogv1.PreviewProductVariantRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotifyContact().GetPhone()},
		},
	}
	previewProductVariantReply, err := s.productVariantValidationPort.PreviewProductVariant(ctx, previewProductVariantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantValidationService/PreviewProductVariant")
	}
	explainAttributeDecisionReply, err := s.attributeValidationClient.ExplainAttributeDecision(ctx, &catalogv1.ExplainAttributeDecisionRequest{
		Context: req.GetContext(),
		Attribute: &catalogv1.Attribute{
			Audit: &commonv1.AuditInfo{
				CreatedBy: previewProductVariantReply.GetProductVariant().GetAudit().GetCreatedBy(),
			},
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetNewOwner().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.AttributeValidationService/ExplainAttributeDecision")
	}
	scoreCategoryRequest := &catalogv1.ScoreCategoryRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Audit: &commonv1.AuditInfo{
						UpdatedAt: previewProductVariantReply.GetProductVariant().GetAudit().GetUpdatedAt(),
					},
					Location: &commonv1.PostalAddress{
						PostalCode: previewProductVariantReply.GetProductVariant().GetAddress().GetPostalCode(),
					},
					Manager: &identityv1.UserRef{
						Email:       req.GetDetails().GetNotifyContact().GetEmail(),
						DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
					},
				}},
			}},
		},
	}
	scoreCategoryReply, err := grpcx.Call(ctx, s.categoryValidationClient.ScoreCategory, scoreCategoryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryValidationService/ScoreCategory")
	}
	return &catalogv1.ReinstateProductVariantResponse{
		ProductVariant: &catalogv1.ProductVariant{
			Owner: &identityv1.UserRef{
				Email:  previewProductVariantReply.GetProductVariant().GetOwner().GetEmail(),
				UserId: scoreCategoryReply.GetCategory().GetManager().GetUserId(),
			},
			Dimensions: &catalogv1.ProductVariantDimensions{
				Length: previewProductVariantReply.GetProductVariant().GetDimensions().GetLength(),
			},
			ProductVariantId: req.GetProductVariantId(),
		},
		Outcome: &catalogv1.ReinstateProductVariantResponse_Outcome{
			Status:      explainAttributeDecisionReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Details: explainAttributeDecisionReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// TerminateProductVariant terminates a product variant.
func (s *ProductVariantLifecycleServer) TerminateProductVariant(ctx context.Context, req *catalogv1.TerminateProductVariantRequest) (*catalogv1.TerminateProductVariantResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product_variant", req.GetProductVariantId(), "terminate_product_variant"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate product variant", "tenant", req.GetContext().GetTenantId())
	normalizeAttributeReply, err := s.attributeValidationClient.NormalizeAttribute(ctx, &catalogv1.NormalizeAttributeRequest{
		Context: req.GetContext(),
		Attribute: &catalogv1.Attribute{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetNewOwner().GetEmail()},
		},
		Details: &catalogv1.NormalizeAttributeRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotifyContact().GetPhone()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.AttributeValidationService/NormalizeAttribute")
	}
	submitStockReservationRequest := &inventoryv1.SubmitStockReservationRequest{
		Context: req.GetContext(),
		StockReservation: &inventoryv1.StockReservation{
			Participants: []*inventoryv1.StockReservationParticipant{&inventoryv1.StockReservationParticipant{
				Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotifyContact().GetName()},
			}},
		},
		Details: &inventoryv1.SubmitStockReservationRequest_Details{
			Approver: &identityv1.UserRef{
				Email: normalizeAttributeReply.GetAttribute().GetOwner().GetEmail(),
			},
		},
	}
	submitStockReservationReply := new(inventoryv1.SubmitStockReservationResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", inventoryStockReservationApprovalServiceName, "SubmitStockReservation"), submitStockReservationRequest, submitStockReservationReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockReservationApprovalService/SubmitStockReservation")
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	syncAttributesRequest := &catalogv1.SyncAttributesRequest{
		Context: req.GetContext(),
		Attribute: &catalogv1.Attribute{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: normalizeAttributeReply.GetAttribute().GetAudit().GetUpdatedAt(),
				UpdatedBy: submitStockReservationReply.GetStockReservation().GetAudit().GetUpdatedBy(),
			},
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetNewOwner().GetEmail()},
			Action: &catalogv1.Attribute_Block{Block: &catalogv1.AttributeBlockAction{
				Reason: normalizeAttributeReply.GetAttribute().GetBlock().GetReason(),
			}},
		},
		Details: &catalogv1.SyncAttributesRequest_Details{Payload: snapshot},
	}
	syncAttributesReply := new(catalogv1.SyncAttributesResponse)
	if err := grpcx.Invoke(ctx, s.conn, catalogv1.AttributeSyncService_SyncAttributes_FullMethodName, syncAttributesRequest, syncAttributesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.AttributeSyncService/SyncAttributes")
	}
	return &catalogv1.TerminateProductVariantResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: normalizeAttributeReply.GetResult().GetCorrelationId(),
			Message:       submitStockReservationReply.GetOutcome().GetMessage(),
			Status:        "ok",
		},
		ProductVariant: &catalogv1.ProductVariant{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: submitStockReservationReply.GetStockReservation().GetAudit().GetUpdatedBy(),
			},
			ProductVariantId: req.GetProductVariantId(),
			Owner:            &identityv1.UserRef{Email: req.GetDetails().GetNewOwner().GetEmail()},
		},
		Outcome: &catalogv1.TerminateProductVariantResponse_Outcome{
			Status:      syncAttributesReply.GetOutcome().GetStatus(),
			EffectiveAt: syncAttributesReply.GetOutcome().GetEffectiveAt(),
		},
	}, nil
}

// RenewProductVariant renews a product variant.
func (s *ProductVariantLifecycleServer) RenewProductVariant(ctx context.Context, req *catalogv1.RenewProductVariantRequest) (*catalogv1.RenewProductVariantResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product_variant", req.GetProductVariantId(), "renew_product_variant"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew product variant", "tenant", req.GetContext().GetTenantId())
	suggestCategoriesRequest := &catalogv1.SuggestCategoriesRequest{
		Context: req.GetContext(),
		Filter:  &catalogv1.SuggestCategoriesRequest_Filter{CreatedAfter: req.GetDetails().GetEffectiveAt()},
	}
	send := s.suggestCategoriesSender(suggestCategoriesRequest)
	suggestCategoriesReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryQueryService/SuggestCategories")
	}
	deleteProductVariantReportRequest := &catalogv1.DeleteProductVariantReportRequest{
		Context:          req.GetContext(),
		ProductVariantId: req.GetProductVariantId(),
		Reason:           req.GetReason(),
	}
	type deleteProductVariantReportResult struct {
		reply *catalogv1.DeleteProductVariantReportResponse
		err   error
	}
	deleteProductVariantReportResults := make(chan deleteProductVariantReportResult, 1)
	go func() {
		reply, err := s.productVariantReportingClient.DeleteProductVariantReport(ctx, deleteProductVariantReportRequest)
		deleteProductVariantReportResults <- deleteProductVariantReportResult{reply, err}
	}()
	var deleteProductVariantReportReply *catalogv1.DeleteProductVariantReportResponse
	select {
	case result := <-deleteProductVariantReportResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.catalog.v1.ProductVariantReportingService/DeleteProductVariantReport")
		}
		deleteProductVariantReportReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &catalogv1.RenewProductVariantResponse{
		Outcome: &catalogv1.RenewProductVariantResponse_Outcome{
			Message:     suggestCategoriesReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: deleteProductVariantReportReply.GetResult().GetCorrelationId(),
			Details:       deleteProductVariantReportReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		ProductVariant: &catalogv1.ProductVariant{ProductVariantId: req.GetProductVariantId()},
	}, nil
}

// ExpireProductVariant expires a product variant.
func (s *ProductVariantLifecycleServer) ExpireProductVariant(ctx context.Context, req *catalogv1.ExpireProductVariantRequest) (*catalogv1.ExpireProductVariantResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product_variant", req.GetProductVariantId(), "expire_product_variant"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire product variant", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	shareBrandReportRequest := &catalogv1.ShareBrandReportRequest{
		Context: req.GetContext(),
		Details: &catalogv1.ShareBrandReportRequest_Details{
			Recipient: &commonv1.ContactInfo{Name: req.GetDetails().GetNotifyContact().GetName()},
			Payload:   packed,
		},
	}
	shareBrandReportReply := new(catalogv1.ShareBrandReportResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.catalog.v1.BrandReportingService/ShareBrandReport", shareBrandReportRequest, shareBrandReportReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandReportingService/ShareBrandReport")
	}
	countFeatureFlagsRequest := &featureflagsv1.CountFeatureFlagsRequest{
		Context: req.GetContext(),
		Filter: &featureflagsv1.CountFeatureFlagsRequest_Filter{
			Region:       shareBrandReportReply.GetBrand().GetPrimaryAddress().GetRegion(),
			CreatedAfter: shareBrandReportReply.GetBrand().GetPassport().GetExpiresAt(),
			OwnerId:      shareBrandReportReply.GetOutcome().GetReferenceId(),
			Labels:       shareBrandReportReply.GetBrand().GetAttributes(),
		},
	}
	countFeatureFlagsReply := new(featureflagsv1.CountFeatureFlagsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", featureflagsFeatureFlagQueryServiceName, "CountFeatureFlags"), countFeatureFlagsRequest, countFeatureFlagsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/CountFeatureFlags")
	}
	return &catalogv1.ExpireProductVariantResponse{
		Outcome: &catalogv1.ExpireProductVariantResponse_Outcome{
			EffectiveAt: shareBrandReportReply.GetOutcome().GetEffectiveAt(),
			ReferenceId: shareBrandReportReply.GetOutcome().GetReferenceId(),
		},
		Result: &commonv1.OperationResult{
			Message:       countFeatureFlagsReply.GetResult().GetMessage(),
			CorrelationId: countFeatureFlagsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		ProductVariant: &catalogv1.ProductVariant{
			ProductVariantId: req.GetProductVariantId(),
			Owner:            &identityv1.UserRef{Email: req.GetDetails().GetNewOwner().GetEmail()},
		},
	}, nil
}

// GetProductVariantLifecycle returns a product variant.
func (s *ProductVariantLifecycleServer) GetProductVariantLifecycle(ctx context.Context, req *catalogv1.GetProductVariantLifecycleRequest) (*catalogv1.GetProductVariantLifecycleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetProductVariantLifecycle", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product_variant", req.GetProductVariantId(), "get_product_variant_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get product variant lifecycle", "tenant", req.GetContext().GetTenantId())
	return &catalogv1.GetProductVariantLifecycleResponse{
		ProductVariant: &catalogv1.ProductVariant{ProductVariantId: req.GetProductVariantId()},
		Result:         &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScheduleProductVariantExpiry schedules a product variant.
func (s *ProductVariantLifecycleServer) ScheduleProductVariantExpiry(ctx context.Context, req *catalogv1.ScheduleProductVariantExpiryRequest) (*catalogv1.ScheduleProductVariantExpiryResponse, error) {
	if err := s.store.SaveProductVariant(ctx, req.GetContext().GetTenantId(), req.GetProductVariant()); err != nil {
		return nil, grpcx.Wrap(err, "save product variant")
	}
	s.log.InfoContext(ctx, "schedule product variant expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductVariant().GetAudit().GetEtag())
	simulateProductRequest := &catalogv1.SimulateProductRequest{
		Context: req.GetContext(),
		Details: &catalogv1.SimulateProductRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient:   req.GetProductVariant().GetAddress().GetRecipient(),
				CountryCode: req.GetProductVariant().GetAddress().GetCountryCode(),
			},
		},
		Product: &catalogv1.Product{
			Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{
				Locality: req.GetProductVariant().GetAddress().GetLocality(),
			}},
		},
	}
	simulateProductReply, err := s.meteredProductCalculation.SimulateProduct(ctx, simulateProductRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductCalculationService/SimulateProduct")
	}
	return &catalogv1.ScheduleProductVariantExpiryResponse{
		ProductVariant: &catalogv1.ProductVariant{
			Dimensions: &catalogv1.ProductVariantDimensions{
				Unit:   simulateProductReply.GetProduct().GetDimensions().GetUnit(),
				Weight: simulateProductReply.GetProduct().GetDimensions().GetWeight(),
			},
			ProductVariantId: req.GetProductVariantId(),
			Capacity:         req.GetProductVariant().GetCapacity(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// suggestCategoriesSender returns a function that sends one request to
// acme.catalog.v1.CategoryQueryService/SuggestCategories.
func (s *ProductVariantLifecycleServer) suggestCategoriesSender(request *catalogv1.SuggestCategoriesRequest) func(context.Context) (*catalogv1.SuggestCategoriesResponse, error) {
	return func(ctx context.Context) (*catalogv1.SuggestCategoriesResponse, error) {
		return s.categoryQueryClient.SuggestCategories(ctx, request)
	}
}
