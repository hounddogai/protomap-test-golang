package catalog

import (
	"context"
	"errors"
	"io"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	configurationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/configuration/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// BrandApprovalServer implements `acme.catalog.v1.BrandApprovalService`.
// Routes brands through approval, with escalation and delegation.
type BrandApprovalServer struct {
	catalogv1.UnimplementedBrandApprovalServiceServer
	store                           *Store
	log                             *slog.Logger
	brandLifecycleClient            catalogv1.BrandLifecycleServiceClient
	attributeReportingClient        catalogv1.AttributeReportingServiceClient
	auditEventValidationClient      auditv1.AuditEventValidationServiceClient
	productVariantCalculationClient catalogv1.ProductVariantCalculationServiceClient
	configSetClient                 configurationv1.ConfigSetServiceClient
	featureFlagValidationClient     featureflagsv1.FeatureFlagValidationServiceClient
	conn                            *grpc.ClientConn
	meteredSearchIndexHistory       meteredSearchIndexHistory
	storageBinWorkflowPort          storageBinWorkflowPort
	brandLifecyclePort              brandLifecyclePort
	searchIndexHistoryClient        searchv1.SearchIndexHistoryServiceClient
	categoryWorkflowClient          catalogv1.CategoryWorkflowServiceClient
	redemptionSyncClient            promotionsv1.RedemptionSyncServiceClient
	brandReportingClient            catalogv1.BrandReportingServiceClient
	placementWorkflowClient         merchandisingv1.PlacementWorkflowServiceClient
	attributeSyncClient             catalogv1.AttributeSyncServiceClient
}

// NewBrandApprovalServer wires a BrandApprovalServer.
func NewBrandApprovalServer(deps platform.Deps) *BrandApprovalServer {
	return &BrandApprovalServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.catalog.v1.BrandApprovalService"),
		brandLifecycleClient:            catalogv1.NewBrandLifecycleServiceClient(deps.Conn),
		attributeReportingClient:        catalogv1.NewAttributeReportingServiceClient(deps.Conn),
		auditEventValidationClient:      auditv1.NewAuditEventValidationServiceClient(deps.Conn),
		productVariantCalculationClient: catalogv1.NewProductVariantCalculationServiceClient(deps.Conn),
		configSetClient:                 configurationv1.NewConfigSetServiceClient(deps.Conn),
		featureFlagValidationClient:     featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn),
		conn:                            deps.Conn,
		meteredSearchIndexHistory:       meteredSearchIndexHistory{SearchIndexHistoryServiceClient: searchv1.NewSearchIndexHistoryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		storageBinWorkflowPort:          newStorageBinWorkflowPort(deps),
		brandLifecyclePort:              newBrandLifecyclePort(deps),
		searchIndexHistoryClient:        searchv1.NewSearchIndexHistoryServiceClient(deps.Conn),
		categoryWorkflowClient:          catalogv1.NewCategoryWorkflowServiceClient(deps.Conn),
		redemptionSyncClient:            promotionsv1.NewRedemptionSyncServiceClient(deps.Conn),
		brandReportingClient:            catalogv1.NewBrandReportingServiceClient(deps.Conn),
		placementWorkflowClient:         merchandisingv1.NewPlacementWorkflowServiceClient(deps.Conn),
		attributeSyncClient:             catalogv1.NewAttributeSyncServiceClient(deps.Conn),
	}
}

// SubmitBrand submits a brand.
func (s *BrandApprovalServer) SubmitBrand(ctx context.Context, req *catalogv1.SubmitBrandRequest) (*catalogv1.SubmitBrandResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveBrand(ctx, req.GetContext().GetTenantId(), req.GetBrand()); err != nil {
		return nil, grpcx.Wrap(err, "save brand")
	}
	s.log.InfoContext(ctx, "submit brand", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBrand().GetAudit().GetEtag())
	getBrandLifecycleRequest := &catalogv1.GetBrandLifecycleRequest{
		Context: req.GetContext(),
		BrandId: req.GetBrand().GetBrandId(),
	}
	send := s.getBrandLifecycleSender(getBrandLifecycleRequest)
	getBrandLifecycleReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandLifecycleService/GetBrandLifecycle")
	}
	listAttributeReportsRequest := &catalogv1.ListAttributeReportsRequest{
		Context: req.GetContext(),
		Filter: &catalogv1.ListAttributeReportsRequest_Filter{
			Region:       req.GetBrand().GetPrimaryAddress().GetRegion(),
			CreatedAfter: getBrandLifecycleReply.GetBrand().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetDetails().GetApprover().GetUserId(),
		},
	}
	listAttributeReportsReply, err := catalogv1.AttributeReportingServiceClient.ListAttributeReports(s.attributeReportingClient, ctx, listAttributeReportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.AttributeReportingService/ListAttributeReports")
	}
	return &catalogv1.SubmitBrandResponse{
		Brand: &catalogv1.Brand{
			Contact: &commonv1.ContactInfo{
				Email: getBrandLifecycleReply.GetBrand().GetRelationshipManager().GetEmail(),
			},
			RelationshipManager: &identityv1.UserRef{
				Email: getBrandLifecycleReply.GetBrand().GetContact().GetEmail(),
			},
			BrandId:        req.GetBrandId(),
			PrimaryAddress: req.GetBrand().GetPrimaryAddress(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: listAttributeReportsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// ApproveBrand approves a brand.
func (s *BrandApprovalServer) ApproveBrand(ctx context.Context, req *catalogv1.ApproveBrandRequest) (*catalogv1.ApproveBrandResponse, error) {
	if err := s.store.SaveBrand(ctx, req.GetContext().GetTenantId(), req.GetBrand()); err != nil {
		return nil, grpcx.Wrap(err, "save brand")
	}
	s.log.InfoContext(ctx, "approve brand", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBrand().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetBrand()).(*catalogv1.Brand)
	validateAuditEventReply, err := s.auditEventValidationClient.ValidateAuditEvent(ctx, &auditv1.ValidateAuditEventRequest{
		Context: req.GetContext(),
		AuditEvent: &auditv1.AuditEvent{
			Subject: &auditv1.AuditTrailRef{
				DisplayName: req.GetBrand().GetRelationshipManager().GetDisplayName(),
			},
		},
		Details: &auditv1.ValidateAuditEventRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: req.GetBrand().GetRelationshipManager().GetEmail(),
				Phone: cloned.GetContact().GetPhone(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventValidationService/ValidateAuditEvent")
	}
	cloned2 := proto.Clone(req.GetBrand()).(*catalogv1.Brand)
	calculateProductVariantReply, err := s.productVariantCalculationClient.CalculateProductVariant(ctx, &catalogv1.CalculateProductVariantRequest{
		Context: req.GetContext(),
		ProductVariant: &catalogv1.ProductVariant{
			Owner:   &identityv1.UserRef{UserId: req.GetBrand().GetRelationshipManager().GetUserId()},
			Locator: &catalogv1.ProductVariant_Address{Address: &commonv1.PostalAddress{Lines: cloned2.GetPrimaryAddress().GetLines()}},
			Audit:   &commonv1.AuditInfo{CreatedAt: req.GetBrand().GetAudit().GetCreatedAt()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantCalculationService/CalculateProductVariant")
	}
	return &catalogv1.ApproveBrandResponse{
		Result: &commonv1.OperationResult{Status: validateAuditEventReply.GetResult().GetStatus()},
		Outcome: &catalogv1.ApproveBrandResponse_Outcome{
			Message: calculateProductVariantReply.GetResult().GetMessage(),
		},
		Brand: &catalogv1.Brand{
			BrandId: req.GetBrandId(),
			Status:  req.GetBrand().GetStatus(),
		},
	}, nil
}

// EscalateBrand escalates a brand.
func (s *BrandApprovalServer) EscalateBrand(ctx context.Context, req *catalogv1.EscalateBrandRequest) (*catalogv1.EscalateBrandResponse, error) {
	if err := s.store.SaveBrand(ctx, req.GetContext().GetTenantId(), req.GetBrand()); err != nil {
		return nil, grpcx.Wrap(err, "save brand")
	}
	s.log.InfoContext(ctx, "escalate brand", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBrand().GetAudit().GetEtag())
	batchGetConfigSetsRequest := &configurationv1.BatchGetConfigSetsRequest{Context: req.GetContext()}
	batchGetConfigSetsReply, err := grpcx.Call(ctx, s.configSetClient.BatchGetConfigSets, batchGetConfigSetsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.configuration.v1.ConfigSetService/BatchGetConfigSets")
	}
	cloned := proto.Clone(req.GetDetails()).(*catalogv1.EscalateBrandRequest_Details)
	validateFeatureFlagRequest := &featureflagsv1.ValidateFeatureFlagRequest{
		Context: req.GetContext(),
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Owner: &identityv1.UserRef{
				DisplayName: cloned.GetApprover().GetDisplayName(),
				UserId:      req.GetDetails().GetApprover().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetBrand().GetAudit().GetUpdatedAt(),
				CreatedAt: req.GetBrand().GetAudit().GetCreatedAt(),
			},
		},
	}
	type validateFeatureFlagResult struct {
		reply *featureflagsv1.ValidateFeatureFlagResponse
		err   error
	}
	validateFeatureFlagResults := make(chan validateFeatureFlagResult, 1)
	go func() {
		reply, err := s.featureFlagValidationClient.ValidateFeatureFlag(ctx, validateFeatureFlagRequest)
		validateFeatureFlagResults <- validateFeatureFlagResult{reply, err}
	}()
	var validateFeatureFlagReply *featureflagsv1.ValidateFeatureFlagResponse
	select {
	case result := <-validateFeatureFlagResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.featureflags.v1.FeatureFlagValidationService/ValidateFeatureFlag")
		}
		validateFeatureFlagReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	cloned2 := proto.Clone(req.GetBrand()).(*catalogv1.Brand)
	scoreAuditEventRequest := &auditv1.ScoreAuditEventRequest{
		Context: req.GetContext(),
		Details: &auditv1.ScoreAuditEventRequest_Details{
			Destination: &commonv1.PostalAddress{Recipient: cloned2.GetPrimaryAddress().GetRecipient()},
		},
	}
	scoreAuditEventReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*auditv1.ScoreAuditEventResponse, error) {
		return s.auditEventValidationClient.ScoreAuditEvent(ctx, scoreAuditEventRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventValidationService/ScoreAuditEvent")
	}
	submitPromotionRequest := &promotionsv1.SubmitPromotionRequest{
		Context: req.GetContext(),
		Promotion: &promotionsv1.Promotion{
			Owner: &identityv1.UserRef{Email: req.GetBrand().GetRelationshipManager().GetEmail()},
		},
	}
	submitPromotionReply := new(promotionsv1.SubmitPromotionResponse)
	if err := grpc.Invoke(ctx, "/acme.promotions.v1.PromotionApprovalService/SubmitPromotion", submitPromotionRequest, submitPromotionReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionApprovalService/SubmitPromotion")
	}
	return &catalogv1.EscalateBrandResponse{
		Outcome: &catalogv1.EscalateBrandResponse_Outcome{Status: batchGetConfigSetsReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			Status:        validateFeatureFlagReply.GetOutcome().GetStatus(),
			CorrelationId: scoreAuditEventReply.GetAuditEvent().GetCorrelationId(),
			Message:       scoreAuditEventReply.GetResult().GetMessage(),
		},
		Brand: &catalogv1.Brand{
			BrandId: req.GetBrandId(),
			Name:    req.GetBrand().GetName(),
		},
	}, nil
}

// ListPendingBrands lists brands a page at a time.
func (s *BrandApprovalServer) ListPendingBrands(ctx context.Context, req *catalogv1.ListPendingBrandsRequest) (*catalogv1.ListPendingBrandsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list pending brands", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "region", req.GetFilter().GetRegion())
	annotateSearchIndexVersionRequest := &searchv1.AnnotateSearchIndexVersionRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Locator: &searchv1.SearchIndex_Address{Address: &commonv1.PostalAddress{Region: tenancy.Value(ctx, "region")}},
			Owner:   &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	annotateSearchIndexVersionReply, err := s.meteredSearchIndexHistory.AnnotateSearchIndexVersion(ctx, annotateSearchIndexVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexHistoryService/AnnotateSearchIndexVersion")
	}
	startStorageBinWorkflowRequest := &warehousingv1.StartStorageBinWorkflowRequest{
		Context: req.GetContext(),
		StorageBin: &warehousingv1.StorageBin{
			Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
				Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
					Audit: &commonv1.AuditInfo{
						UpdatedBy: annotateSearchIndexVersionReply.GetSearchIndex().GetAudit().GetUpdatedBy(),
						Etag:      annotateSearchIndexVersionReply.GetSearchIndex().GetAudit().GetEtag(),
					},
					Location: &commonv1.PostalAddress{
						Lines: annotateSearchIndexVersionReply.GetSearchIndex().GetAddress().GetLines(),
					},
					Manager: &identityv1.UserRef{
						Email: annotateSearchIndexVersionReply.GetSearchIndex().GetOwner().GetEmail(),
					},
				}},
			}},
		},
	}
	startStorageBinWorkflowReply, err := s.storageBinWorkflowPort.StartStorageBinWorkflow(ctx, startStorageBinWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinWorkflowService/StartStorageBinWorkflow")
	}
	return &catalogv1.ListPendingBrandsResponse{
		Brands: []*catalogv1.Brand{&catalogv1.Brand{
			RelationshipManager: &identityv1.UserRef{
				Email: annotateSearchIndexVersionReply.GetSearchIndex().GetOwner().GetEmail(),
			},
			PrimaryAddress: &commonv1.PostalAddress{
				CountryCode: annotateSearchIndexVersionReply.GetSearchIndex().GetAddress().GetCountryCode(),
			},
		}},
		Result: &commonv1.OperationResult{
			Details: startStorageBinWorkflowReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// DelegateBrandApproval delegates a brand.
func (s *BrandApprovalServer) DelegateBrandApproval(ctx context.Context, req *catalogv1.DelegateBrandApprovalRequest) (*catalogv1.DelegateBrandApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveBrand(ctx, req.GetContext().GetTenantId(), req.GetBrand()); err != nil {
		return nil, grpcx.Wrap(err, "save brand")
	}
	s.log.InfoContext(ctx, "delegate brand approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBrand().GetAudit().GetEtag())
	expireBrandRequest := &catalogv1.ExpireBrandRequest{
		Context: req.GetContext(),
		BrandId: req.GetBrand().GetBrandId(),
		Details: &catalogv1.ExpireBrandRequest_Details{
			NewOwner: &identityv1.UserRef{
				DisplayName: req.GetBrand().GetRelationshipManager().GetDisplayName(),
				Email:       req.GetBrand().GetContact().GetEmail(),
			},
		},
	}
	expireBrandReply, err := s.brandLifecyclePort.ExpireBrand(ctx, expireBrandRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandLifecycleService/ExpireBrand")
	}
	brandCache := cache.New[string, *catalogv1.Brand](1)
	brandCache.Put(req.GetContext().GetTraceId(), req.GetBrand())
	cachedBrand, _ := brandCache.Get(req.GetContext().GetTraceId())
	listSearchIndexVersionsRequest := &searchv1.ListSearchIndexVersionsRequest{}
	listSearchIndexVersionsRequest.Context = req.GetContext()
	listSearchIndexVersionsRequest.Filter = &searchv1.ListSearchIndexVersionsRequest_Filter{
		Region:       expireBrandReply.GetBrand().GetPrimaryAddress().GetRegion(),
		CreatedAfter: cachedBrand.GetAudit().GetUpdatedAt(),
	}
	listSearchIndexVersionsReply, err := s.searchIndexHistoryClient.ListSearchIndexVersions(ctx, listSearchIndexVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexHistoryService/ListSearchIndexVersions")
	}
	signalCategoryWorkflowRequest := &catalogv1.SignalCategoryWorkflowRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Manager: &identityv1.UserRef{
						DisplayName: req.GetBrand().GetRelationshipManager().GetDisplayName(),
					},
					Audit: &commonv1.AuditInfo{
						UpdatedBy: expireBrandReply.GetBrand().GetAudit().GetUpdatedBy(),
					},
				}},
			}},
		},
	}
	type signalCategoryWorkflowResult struct {
		reply *catalogv1.SignalCategoryWorkflowResponse
		err   error
	}
	signalCategoryWorkflowResults := make(chan signalCategoryWorkflowResult, 1)
	go func() {
		reply, err := s.categoryWorkflowClient.SignalCategoryWorkflow(ctx, signalCategoryWorkflowRequest)
		signalCategoryWorkflowResults <- signalCategoryWorkflowResult{reply, err}
	}()
	var signalCategoryWorkflowReply *catalogv1.SignalCategoryWorkflowResponse
	select {
	case result := <-signalCategoryWorkflowResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.catalog.v1.CategoryWorkflowService/SignalCategoryWorkflow")
		}
		signalCategoryWorkflowReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &catalogv1.DelegateBrandApprovalResponse{
		Result: &commonv1.OperationResult{
			Status:       expireBrandReply.GetResult().GetStatus(),
			ReferenceIds: listSearchIndexVersionsReply.GetResult().GetReferenceIds(),
		},
		Brand: &catalogv1.Brand{
			PrimaryAddress: &commonv1.PostalAddress{
				Locality:   expireBrandReply.GetBrand().GetPrimaryAddress().GetLocality(),
				PostalCode: signalCategoryWorkflowReply.GetCategory().GetLocation().GetPostalCode(),
			},
			BrandId: req.GetBrandId(),
			Status:  req.GetBrand().GetStatus(),
		},
	}, nil
}

// RecallBrand recalls a brand.
func (s *BrandApprovalServer) RecallBrand(ctx context.Context, req *catalogv1.RecallBrandRequest) (*catalogv1.RecallBrandResponse, error) {
	if err := s.store.SaveBrand(ctx, req.GetContext().GetTenantId(), req.GetBrand()); err != nil {
		return nil, grpcx.Wrap(err, "save brand")
	}
	s.log.InfoContext(ctx, "recall brand", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBrand().GetAudit().GetEtag())
	reconcileRedemptionsRequest := buildReconcileRedemptionsRequestForRecallBrand(req)
	reconcileRedemptionsReply, err := s.redemptionSyncClient.ReconcileRedemptions(ctx, reconcileRedemptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionSyncService/ReconcileRedemptions")
	}
	scheduleBrandReportReply, err := s.brandReportingClient.ScheduleBrandReport(ctx, &catalogv1.ScheduleBrandReportRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: req.GetAnnotations(),
		BrandId:     req.GetBrand().GetBrandId(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandReportingService/ScheduleBrandReport")
	}
	return &catalogv1.RecallBrandResponse{
		Outcome: &catalogv1.RecallBrandResponse_Outcome{
			ReferenceId: reconcileRedemptionsReply.GetOutcome().GetReferenceId(),
		},
		Result: &commonv1.OperationResult{
			Status:        reconcileRedemptionsReply.GetResult().GetStatus(),
			CorrelationId: scheduleBrandReportReply.GetResult().GetCorrelationId(),
			ReferenceIds:  scheduleBrandReportReply.GetResult().GetReferenceIds(),
		},
		Brand: &catalogv1.Brand{
			BrandId:        req.GetBrandId(),
			PrimaryAddress: req.GetBrand().GetPrimaryAddress(),
		},
	}, nil
}

// GetBrandApprovalStatus returns a brand.
func (s *BrandApprovalServer) GetBrandApprovalStatus(ctx context.Context, req *catalogv1.GetBrandApprovalStatusRequest) (*catalogv1.GetBrandApprovalStatusResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "brand", req.GetBrandId(), "get_brand_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get brand approval status", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	listPlacementWorkflowsReply, err := s.placementWorkflowClient.ListPlacementWorkflows(ctx, &merchandisingv1.ListPlacementWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &merchandisingv1.ListPlacementWorkflowsRequest_Filter{
			OwnerId:      req.GetBrandId(),
			CreatedAfter: req.GetContext().GetDeadline(),
			Labels:       baggage,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementWorkflowService/ListPlacementWorkflows")
	}
	return &catalogv1.GetBrandApprovalStatusResponse{
		Result: &commonv1.OperationResult{
			Message: listPlacementWorkflowsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Brand: &catalogv1.Brand{BrandId: req.GetBrandId()},
	}, nil
}

// CommentBrandApproval comments a brand.
func (s *BrandApprovalServer) CommentBrandApproval(ctx context.Context, req *catalogv1.CommentBrandApprovalRequest) (*catalogv1.CommentBrandApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CommentBrandApproval", "error", err)
		return nil, err
	}
	if err := s.store.SaveBrand(ctx, req.GetContext().GetTenantId(), req.GetBrand()); err != nil {
		return nil, grpcx.Wrap(err, "save brand")
	}
	s.log.InfoContext(ctx, "comment brand approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBrand().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetBrand()).(*catalogv1.Brand)
	streamAttributeChangesStream, err := s.attributeSyncClient.StreamAttributeChanges(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.AttributeSyncService/StreamAttributeChanges")
	}
	if err := streamAttributeChangesStream.Send(&catalogv1.StreamAttributeChangesRequest{
		Context: req.GetContext(),
		Frame: &catalogv1.StreamAttributeChangesRequest_Change{Change: &catalogv1.Attribute{
			Owner: &identityv1.UserRef{UserId: req.GetBrand().GetRelationshipManager().GetUserId()},
			Audit: &commonv1.AuditInfo{UpdatedAt: cloned.GetAudit().GetUpdatedAt()},
		}},
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.AttributeSyncService/StreamAttributeChanges")
	}
	if err := streamAttributeChangesStream.CloseSend(); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.AttributeSyncService/StreamAttributeChanges")
	}
	acknowledged, err := streamAttributeChangesStream.Recv()
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.AttributeSyncService/StreamAttributeChanges")
	}
	_ = acknowledged
	encoded, err := protojson.Marshal(req.GetBrand())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded catalogv1.Brand
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	pauseCategoryWorkflowReply, err := s.categoryWorkflowClient.PauseCategoryWorkflow(ctx, &catalogv1.PauseCategoryWorkflowRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Audit:    &commonv1.AuditInfo{CreatedBy: req.GetBrand().GetAudit().GetCreatedBy()},
					Location: &commonv1.PostalAddress{Lines: (&decoded).GetPrimaryAddress().GetLines()},
				}},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryWorkflowService/PauseCategoryWorkflow")
	}
	return &catalogv1.CommentBrandApprovalResponse{
		Brand: &catalogv1.Brand{
			PrimaryAddress: &commonv1.PostalAddress{
				PostalCode: pauseCategoryWorkflowReply.GetCategory().GetLocation().GetPostalCode(),
			},
			BrandId: req.GetBrandId(),
			Contact: &commonv1.ContactInfo{Name: req.GetBrand().GetName()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// getBrandLifecycleSender returns a function that sends one request to
// acme.catalog.v1.BrandLifecycleService/GetBrandLifecycle.
func (s *BrandApprovalServer) getBrandLifecycleSender(request *catalogv1.GetBrandLifecycleRequest) func(context.Context) (*catalogv1.GetBrandLifecycleResponse, error) {
	return func(ctx context.Context) (*catalogv1.GetBrandLifecycleResponse, error) {
		return s.brandLifecycleClient.GetBrandLifecycle(ctx, request)
	}
}
