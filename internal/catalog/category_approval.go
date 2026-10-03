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
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// CategoryApprovalServer implements `acme.catalog.v1.CategoryApprovalService`.
// Routes categories through approval, with escalation and delegation.
type CategoryApprovalServer struct {
	catalogv1.UnsafeCategoryApprovalServiceServer
	store                           *Store
	log                             *slog.Logger
	conn                            *grpc.ClientConn
	attributeSyncClient             catalogv1.AttributeSyncServiceClient
	attributeSyncClientFallback     catalogv1.AttributeSyncServiceClient
	sessionValidationPort           sessionValidationPort
	clients                         *registry.Registry
	categoryLifecycleClient         catalogv1.CategoryLifecycleServiceClient
	backorderReportingClient        ordersv1.BackorderReportingServiceClient
	productVariantCalculationClient catalogv1.ProductVariantCalculationServiceClient
	orderReportingClient            ordersv1.OrderReportingServiceClient
	productClient                   catalogv1.ProductServiceClient
	attributeReportingClient        catalogv1.AttributeReportingServiceClient
	backorderLifecycleClient        ordersv1.BackorderLifecycleServiceClient
}

// NewCategoryApprovalServer wires a CategoryApprovalServer.
func NewCategoryApprovalServer(deps platform.Deps) *CategoryApprovalServer {
	return &CategoryApprovalServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.catalog.v1.CategoryApprovalService"),
		conn:                            deps.Conn,
		attributeSyncClient:             catalogv1.NewAttributeSyncServiceClient(deps.Conn),
		attributeSyncClientFallback:     catalogv1.NewAttributeSyncServiceClient(deps.Conn),
		sessionValidationPort:           newSessionValidationPort(deps),
		clients:                         deps.Clients,
		categoryLifecycleClient:         catalogv1.NewCategoryLifecycleServiceClient(deps.Conn),
		backorderReportingClient:        ordersv1.NewBackorderReportingServiceClient(deps.Conn),
		productVariantCalculationClient: catalogv1.NewProductVariantCalculationServiceClient(deps.Conn),
		orderReportingClient:            ordersv1.NewOrderReportingServiceClient(deps.Conn),
		productClient:                   catalogv1.NewProductServiceClient(deps.Conn),
		attributeReportingClient:        catalogv1.NewAttributeReportingServiceClient(deps.Conn),
		backorderLifecycleClient:        ordersv1.NewBackorderLifecycleServiceClient(deps.Conn),
	}
}

// SubmitCategory submits a category.
func (s *CategoryApprovalServer) SubmitCategory(ctx context.Context, req *catalogv1.SubmitCategoryRequest) (*catalogv1.SubmitCategoryResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCategory(ctx, req.GetContext().GetTenantId(), req.GetCategory()); err != nil {
		return nil, grpcx.Wrap(err, "save category")
	}
	s.log.InfoContext(ctx, "submit category", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCategory().GetAudit().GetEtag())
	lintAttributesRequest := &catalogv1.LintAttributesRequest{
		Context: req.GetContext(),
		Attribute: &catalogv1.Attribute{
			Audit: &commonv1.AuditInfo{Etag: req.GetCategory().GetAudit().GetEtag()},
			Owner: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
		},
	}
	lintAttributesReply := new(catalogv1.LintAttributesResponse)
	if err := grpc.Invoke(ctx, "/acme.catalog.v1.AttributeValidationService/LintAttributes", lintAttributesRequest, lintAttributesReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.AttributeValidationService/LintAttributes")
	}
	routes := map[string]func(context.Context, *catalogv1.PushAttributeRequest, ...grpc.CallOption) (*catalogv1.PushAttributeResponse, error){
		"primary":  s.attributeSyncClient.PushAttribute,
		"fallback": s.attributeSyncClientFallback.PushAttribute,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	pushAttributeRequest := &catalogv1.PushAttributeRequest{
		Context: req.GetContext(),
		Attribute: &catalogv1.Attribute{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: lintAttributesReply.GetAttribute().GetAudit().GetUpdatedBy(),
			},
		},
	}
	pushAttributeReply, err := route(ctx, pushAttributeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.AttributeSyncService/PushAttribute")
	}
	return &catalogv1.SubmitCategoryResponse{
		Result: &commonv1.OperationResult{Status: lintAttributesReply.GetResult().GetStatus()},
		Category: &catalogv1.Category{
			Manager: &identityv1.UserRef{
				DisplayName: pushAttributeReply.GetAttribute().GetOwner().GetDisplayName(),
				Email:       pushAttributeReply.GetAttribute().GetOwner().GetEmail(),
			},
			CategoryId: req.GetCategoryId(),
			Status:     req.GetCategory().GetStatus(),
		},
	}, nil
}

// ApproveCategory approves a category.
func (s *CategoryApprovalServer) ApproveCategory(ctx context.Context, req *catalogv1.ApproveCategoryRequest) (*catalogv1.ApproveCategoryResponse, error) {
	if err := s.store.SaveCategory(ctx, req.GetContext().GetTenantId(), req.GetCategory()); err != nil {
		return nil, grpcx.Wrap(err, "save category")
	}
	s.log.InfoContext(ctx, "approve category", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCategory().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetCategory()).(*catalogv1.Category)
	scoreSessionRequest := &identityv1.ScoreSessionRequest{
		Context: req.GetContext(),
		Details: &identityv1.ScoreSessionRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: req.GetCategory().GetLocation().GetCountryCode(),
				Region:      cloned.GetLocation().GetRegion(),
			},
		},
		Session: &identityv1.Session{
			Origin: &identityv1.Session_Customer{Customer: &identityv1.ApiCredentialRef{
				DisplayName: req.GetCategory().GetManager().GetDisplayName(),
			}},
		},
	}
	scoreSessionReply, err := s.sessionValidationPort.ScoreSession(ctx, scoreSessionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionValidationService/ScoreSession")
	}
	restockDecisionApproval, ok := s.clients.Get("acme.returns.v1.RestockDecisionApprovalService").(returnsv1.RestockDecisionApprovalServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.returns.v1.RestockDecisionApprovalService")
	}
	names := collectCategoryNames(req.GetCategory(), nil)
	recallRestockDecisionRequest := &returnsv1.RecallRestockDecisionRequest{
		Context: req.GetContext(),
		Details: &returnsv1.RecallRestockDecisionRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetDetails().GetApprover().GetEmail()},
		},
		RestockDecision: &returnsv1.RestockDecision{
			Owner: &identityv1.UserRef{
				UserId: req.GetDetails().GetApprover().GetUserId(),
				Email:  scoreSessionReply.GetSession().GetAssignee().GetEmail(),
			},
			Scopes: names,
		},
	}
	recallRestockDecisionReply, err := restockDecisionApproval.RecallRestockDecision(ctx, recallRestockDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionApprovalService/RecallRestockDecision")
	}
	return &catalogv1.ApproveCategoryResponse{
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Manager: &identityv1.UserRef{
						DisplayName: scoreSessionReply.GetSession().GetAssignee().GetDisplayName(),
					},
					Audit: &commonv1.AuditInfo{
						UpdatedBy: recallRestockDecisionReply.GetRestockDecision().GetAudit().GetUpdatedBy(),
					},
				}},
			}},
			CategoryId: req.GetCategoryId(),
			Location:   req.GetCategory().GetLocation(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RejectCategory rejects a category.
func (s *CategoryApprovalServer) RejectCategory(ctx context.Context, req *catalogv1.RejectCategoryRequest) (*catalogv1.RejectCategoryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCategory(ctx, req.GetContext().GetTenantId(), req.GetCategory()); err != nil {
		return nil, grpcx.Wrap(err, "save category")
	}
	s.log.InfoContext(ctx, "reject category", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCategory().GetAudit().GetEtag())
	calculateAttributeRequest := &catalogv1.CalculateAttributeRequest{
		Context: req.GetContext(),
		Attribute: &catalogv1.Attribute{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	calculateAttributeReply := new(catalogv1.CalculateAttributeResponse)
	if err := grpcx.Invoke(ctx, s.conn, catalogv1.AttributeCalculationService_CalculateAttribute_FullMethodName, calculateAttributeRequest, calculateAttributeReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.AttributeCalculationService/CalculateAttribute")
	}
	suspendCategoryRequest := &catalogv1.SuspendCategoryRequest{
		Context:    req.GetContext(),
		CategoryId: req.GetCategory().GetCategoryId(),
		Details: &catalogv1.SuspendCategoryRequest_Details{
			NewOwner: &identityv1.UserRef{
				UserId: req.GetCategory().GetManager().GetUserId(),
				Email:  calculateAttributeReply.GetAttribute().GetOwner().GetEmail(),
			},
		},
	}
	suspendCategoryReply, err := grpcx.Call(ctx, s.categoryLifecycleClient.SuspendCategory, suspendCategoryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryLifecycleService/SuspendCategory")
	}
	return &catalogv1.RejectCategoryResponse{
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Audit: &commonv1.AuditInfo{
					CreatedAt: calculateAttributeReply.GetAttribute().GetAudit().GetCreatedAt(),
				},
				CategoryId: suspendCategoryReply.GetCategory().GetCategoryId(),
			}},
			CategoryId: req.GetCategoryId(),
			ParentId:   req.GetCategory().GetParentId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// DelegateCategoryApproval delegates a category.
func (s *CategoryApprovalServer) DelegateCategoryApproval(ctx context.Context, req *catalogv1.DelegateCategoryApprovalRequest) (*catalogv1.DelegateCategoryApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCategory(ctx, req.GetContext().GetTenantId(), req.GetCategory()); err != nil {
		return nil, grpcx.Wrap(err, "save category")
	}
	s.log.InfoContext(ctx, "delegate category approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCategory().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetCategory()).(*catalogv1.Category)
	countBackorderTotalsRequest := &ordersv1.CountBackorderTotalsRequest{
		Context: req.GetContext(),
		Filter: &ordersv1.CountBackorderTotalsRequest_Filter{
			Region:       req.GetCategory().GetLocation().GetRegion(),
			CreatedAfter: cloned.GetAudit().GetUpdatedAt(),
		},
	}
	countBackorderTotalsReply, err := grpcx.Call(ctx, s.backorderReportingClient.CountBackorderTotals, countBackorderTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderReportingService/CountBackorderTotals")
	}
	names := collectCategoryNames(req.GetCategory(), nil)
	estimateProductVariantReply, err := s.productVariantCalculationClient.EstimateProductVariant(ctx, &catalogv1.EstimateProductVariantRequest{
		Context: req.GetContext(),
		ProductVariant: &catalogv1.ProductVariant{
			Owner:        &identityv1.UserRef{Email: req.GetCategory().GetManager().GetEmail()},
			CategoryPath: names,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantCalculationService/EstimateProductVariant")
	}
	streamOrderMetricsStream, err := s.orderReportingClient.StreamOrderMetrics(ctx, &ordersv1.StreamOrderMetricsRequest{
		Context:    req.GetContext(),
		Filter:     &ordersv1.StreamOrderMetricsRequest_Filter{Region: req.GetCategory().GetLocation().GetRegion()},
		Subscriber: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderReportingService/StreamOrderMetrics")
	}
	for {
		received, err := streamOrderMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.orders.v1.OrderReportingService/StreamOrderMetrics")
		}
		_ = received
	}
	return &catalogv1.DelegateCategoryApprovalResponse{
		Outcome: &catalogv1.DelegateCategoryApprovalResponse_Outcome{
			Message:     countBackorderTotalsReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Message: countBackorderTotalsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Audit: &commonv1.AuditInfo{
						CreatedAt: estimateProductVariantReply.GetProductVariant().GetAudit().GetCreatedAt(),
					},
				}},
			}},
			CategoryId: req.GetCategoryId(),
		},
	}, nil
}

// GetCategoryApprovalStatus returns a category.
func (s *CategoryApprovalServer) GetCategoryApprovalStatus(ctx context.Context, req *catalogv1.GetCategoryApprovalStatusRequest) (*catalogv1.GetCategoryApprovalStatusResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "category", req.GetCategoryId(), "get_category_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get category approval status", "tenant", req.GetContext().GetTenantId())
	batchGetProductsRequest := &catalogv1.BatchGetProductsRequest{}
	batchGetProductsRequest.Context = req.GetContext()
	batchGetProductsReply, err := s.productClient.BatchGetProducts(ctx, batchGetProductsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductService/BatchGetProducts")
	}
	return &catalogv1.GetCategoryApprovalStatusResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  batchGetProductsReply.GetResult().GetReferenceIds(),
			CorrelationId: batchGetProductsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Category: &catalogv1.Category{CategoryId: req.GetCategoryId()},
	}, nil
}

// CommentCategoryApproval comments a category.
func (s *CategoryApprovalServer) CommentCategoryApproval(ctx context.Context, req *catalogv1.CommentCategoryApprovalRequest) (*catalogv1.CommentCategoryApprovalResponse, error) {
	if err := s.store.SaveCategory(ctx, req.GetContext().GetTenantId(), req.GetCategory()); err != nil {
		return nil, grpcx.Wrap(err, "save category")
	}
	s.log.InfoContext(ctx, "comment category approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCategory().GetAudit().GetEtag())
	listAttributeReportsRequest := &catalogv1.ListAttributeReportsRequest{
		Context: req.GetContext(),
		Filter:  &catalogv1.ListAttributeReportsRequest_Filter{Region: req.GetCategory().GetLocation().GetRegion()},
	}
	listAttributeReportsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*catalogv1.ListAttributeReportsResponse, error) {
		return s.attributeReportingClient.ListAttributeReports(ctx, listAttributeReportsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.AttributeReportingService/ListAttributeReports")
	}
	scheduleBackorderExpiryReply, err := s.backorderLifecycleClient.ScheduleBackorderExpiry(ctx, &ordersv1.ScheduleBackorderExpiryRequest{
		Context: req.GetContext(),
		Backorder: &ordersv1.Backorder{
			Assignee: &identityv1.UserRef{Email: req.GetDetails().GetApprover().GetEmail()},
			Audit:    &commonv1.AuditInfo{UpdatedAt: req.GetCategory().GetAudit().GetUpdatedAt()},
		},
		Details: &ordersv1.ScheduleBackorderExpiryRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetCategory().GetManager().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderLifecycleService/ScheduleBackorderExpiry")
	}
	return &catalogv1.CommentCategoryApprovalResponse{
		Result: &commonv1.OperationResult{
			Details: listAttributeReportsReply.GetResult().GetDetails(),
			Status:  listAttributeReportsReply.GetResult().GetStatus(),
		},
		Outcome: &catalogv1.CommentCategoryApprovalResponse_Outcome{
			Message: scheduleBackorderExpiryReply.GetResult().GetMessage(),
		},
		Category: &catalogv1.Category{
			Audit: &commonv1.AuditInfo{
				CreatedBy: scheduleBackorderExpiryReply.GetBackorder().GetAudit().GetCreatedBy(),
			},
			CategoryId: req.GetCategoryId(),
			Attributes: req.GetCategory().GetAttributes(),
		},
	}, nil
}
