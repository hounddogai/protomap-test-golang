package catalog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	accountingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/accounting/v1"
	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	dispatchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/dispatch/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	onboardingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/onboarding/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// ProductWorkflowServer implements `acme.catalog.v1.ProductWorkflowService`.
// Runs the long-lived workflow that takes a product from start to finish.
type ProductWorkflowServer struct {
	catalogv1.UnimplementedProductWorkflowServiceServer
	store                         *Store
	log                           *slog.Logger
	conn                          *grpc.ClientConn
	redemptionApprovalOnce        sync.Once
	redemptionApprovalCached      promotionsv1.RedemptionApprovalServiceClient
	clients                       *registry.Registry
	collectionWorkflowClient      merchandisingv1.CollectionWorkflowServiceClient
	chartOfAccountsClient         accountingv1.ChartOfAccountsServiceClient
	brandReportingClient          catalogv1.BrandReportingServiceClient
	promotionWorkflowClient       promotionsv1.PromotionWorkflowServiceClient
	bus                           *eventbus.Bus
	sessionHistoryOnce            sync.Once
	sessionHistoryCached          identityv1.SessionHistoryServiceClient
	ledgerAccountValidationClient ledgerv1.LedgerAccountValidationServiceClient
	productQueryClient            catalogv1.ProductQueryServiceClient
	userValidationClient          identityv1.UserValidationServiceClient
	productVariantReportingClient catalogv1.ProductVariantReportingServiceClient
	meteredProductApproval        meteredProductApproval
	searchIndexAdminClient        searchv1.SearchIndexAdminServiceClient
	meteredDispatchOrder          meteredDispatchOrder
	categoryApprovalClient        catalogv1.CategoryApprovalServiceClient
}

// NewProductWorkflowServer wires a ProductWorkflowServer.
func NewProductWorkflowServer(deps platform.Deps) *ProductWorkflowServer {
	return &ProductWorkflowServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.catalog.v1.ProductWorkflowService"),
		conn:                          deps.Conn,
		clients:                       deps.Clients,
		collectionWorkflowClient:      merchandisingv1.NewCollectionWorkflowServiceClient(deps.Conn),
		chartOfAccountsClient:         accountingv1.NewChartOfAccountsServiceClient(deps.Conn),
		brandReportingClient:          catalogv1.NewBrandReportingServiceClient(deps.Conn),
		promotionWorkflowClient:       promotionsv1.NewPromotionWorkflowServiceClient(deps.Conn),
		bus:                           deps.Bus,
		ledgerAccountValidationClient: ledgerv1.NewLedgerAccountValidationServiceClient(deps.Conn),
		productQueryClient:            catalogv1.NewProductQueryServiceClient(deps.Conn),
		userValidationClient:          identityv1.NewUserValidationServiceClient(deps.Conn),
		productVariantReportingClient: catalogv1.NewProductVariantReportingServiceClient(deps.Conn),
		meteredProductApproval:        meteredProductApproval{ProductApprovalServiceClient: catalogv1.NewProductApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		searchIndexAdminClient:        searchv1.NewSearchIndexAdminServiceClient(deps.Conn),
		meteredDispatchOrder:          meteredDispatchOrder{DispatchOrderServiceClient: dispatchv1.NewDispatchOrderServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		categoryApprovalClient:        catalogv1.NewCategoryApprovalServiceClient(deps.Conn),
	}
}

// StartProductWorkflow starts a product.
func (s *ProductWorkflowServer) StartProductWorkflow(ctx context.Context, req *catalogv1.StartProductWorkflowRequest) (*catalogv1.StartProductWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "start product workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetProduct())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	shareBrandReportRequest := &catalogv1.ShareBrandReportRequest{
		Context: req.GetContext(),
		Details: &catalogv1.ShareBrandReportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				Phone:            req.GetDetails().GetNotify().GetPhone(),
			},
			Payload: packed,
		},
	}
	shareBrandReportReply, err := grpcx.NewClient(s.conn, catalogv1.NewBrandReportingServiceClient).ShareBrandReport(ctx, shareBrandReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandReportingService/ShareBrandReport")
	}
	packed2, err := anypb.New(req.GetProduct())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	delegateRedemptionApprovalRequest := &promotionsv1.DelegateRedemptionApprovalRequest{
		Context: req.GetContext(),
		Details: &promotionsv1.DelegateRedemptionApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				UserId: shareBrandReportReply.GetBrand().GetRelationshipManager().GetUserId(),
			},
		},
		Redemption: &promotionsv1.Redemption{Detail: packed2},
	}
	delegateRedemptionApprovalReply, err := s.redemptionApprovalLazy().DelegateRedemptionApproval(ctx, delegateRedemptionApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionApprovalService/DelegateRedemptionApproval")
	}
	pointsLedgerApproval, err := registry.Lookup[loyaltyv1.PointsLedgerApprovalServiceClient](s.clients, "acme.loyalty.v1.PointsLedgerApprovalService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	listPendingPointsLedgersRequest := &loyaltyv1.ListPendingPointsLedgersRequest{
		Context: req.GetContext(),
		Filter: &loyaltyv1.ListPendingPointsLedgersRequest_Filter{
			Region:       req.GetProduct().GetAddress().GetRegion(),
			CreatedAfter: shareBrandReportReply.GetBrand().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetProduct().GetComponents()["primary"].GetComponentId(),
			Labels:       delegateRedemptionApprovalReply.GetResult().GetDetails(),
		},
	}
	listPendingPointsLedgersReply, err := pointsLedgerApproval.ListPendingPointsLedgers(ctx, listPendingPointsLedgersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerApprovalService/ListPendingPointsLedgers")
	}
	return &catalogv1.StartProductWorkflowResponse{
		Outcome: &catalogv1.StartProductWorkflowResponse_Outcome{Message: shareBrandReportReply.GetOutcome().GetMessage()},
		Product: &catalogv1.Product{
			Owner: &identityv1.UserRef{
				DisplayName: delegateRedemptionApprovalReply.GetRedemption().GetSubject().GetDisplayName(),
			},
			ProductId: req.GetProductId(),
			Code:      req.GetProduct().GetCode(),
		},
		Result: &commonv1.OperationResult{
			Message: listPendingPointsLedgersReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// AdvanceProductWorkflow advances a product.
func (s *ProductWorkflowServer) AdvanceProductWorkflow(ctx context.Context, req *catalogv1.AdvanceProductWorkflowRequest) (*catalogv1.AdvanceProductWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected AdvanceProductWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "advance product workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "country_code", req.GetProduct().GetAddress().GetCountryCode())
	advanceCollectionWorkflowRequest := &merchandisingv1.AdvanceCollectionWorkflowRequest{}
	advanceCollectionWorkflowRequest.Context = req.GetContext()
	advanceCollectionWorkflowRequest.Collection = &merchandisingv1.Collection{
		Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Location: &commonv1.PostalAddress{CountryCode: tenancy.Value(ctx, "country_code")},
				Audit:    &commonv1.AuditInfo{UpdatedBy: req.GetProduct().GetAudit().GetUpdatedBy()},
			}},
		}},
	}
	advanceCollectionWorkflowReply, err := s.collectionWorkflowClient.AdvanceCollectionWorkflow(ctx, advanceCollectionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionWorkflowService/AdvanceCollectionWorkflow")
	}
	listChartOfAccountsesRequest := &accountingv1.ListChartOfAccountsesRequest{
		Context: req.GetContext(),
		Filter: &accountingv1.ListChartOfAccountsesRequest_Filter{
			Region:       advanceCollectionWorkflowReply.GetCollection().GetLocation().GetRegion(),
			CreatedAfter: req.GetProduct().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetProduct().GetProductId(),
		},
	}
	listChartOfAccountsesReply := new(accountingv1.ListChartOfAccountsesResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", accountingChartOfAccountsServiceName, "ListChartOfAccountses"), listChartOfAccountsesRequest, listChartOfAccountsesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.accounting.v1.ChartOfAccountsService/ListChartOfAccountses")
	}
	return &catalogv1.AdvanceProductWorkflowResponse{
		Product: &catalogv1.Product{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: advanceCollectionWorkflowReply.GetCollection().GetAudit().GetUpdatedAt(),
				UpdatedBy: advanceCollectionWorkflowReply.GetCollection().GetAudit().GetUpdatedBy(),
			},
			ProductId: req.GetProductId(),
			Owner:     req.GetProduct().GetOwner(),
		},
		Result: &commonv1.OperationResult{
			Details: listChartOfAccountsesReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// CancelProductWorkflow cancels a product.
func (s *ProductWorkflowServer) CancelProductWorkflow(ctx context.Context, req *catalogv1.CancelProductWorkflowRequest) (*catalogv1.CancelProductWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "cancel product workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetProduct()).(*catalogv1.Product)
	watchChartOfAccountsesStream, err := s.chartOfAccountsClient.WatchChartOfAccountses(ctx, &accountingv1.WatchChartOfAccountsesRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email: cloned.GetOwner().GetEmail(),
			Phone: req.GetDetails().GetNotify().GetPhone(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.accounting.v1.ChartOfAccountsService/WatchChartOfAccountses")
	}
	for {
		received, err := watchChartOfAccountsesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.accounting.v1.ChartOfAccountsService/WatchChartOfAccountses")
		}
		_ = received
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	specs := make(map[string]string, len(req.GetProduct().GetSpecs()))
	for key2, value2 := range req.GetProduct().GetSpecs() {
		specs[key2] = value2
	}
	scheduleBrandReportRequest := &catalogv1.ScheduleBrandReportRequest{}
	scheduleBrandReportRequest.Context = req.GetContext()
	scheduleBrandReportRequest.Annotations = annotations
	scheduleBrandReportRequest.Reason = tenancy.Value(ctx, "reason")
	scheduleBrandReportRequest.Details = &catalogv1.ScheduleBrandReportRequest_Details{Filters: specs}
	scheduleBrandReportReply, err := s.brandReportingClient.ScheduleBrandReport(ctx, scheduleBrandReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandReportingService/ScheduleBrandReport")
	}
	resumePromotionWorkflowReply, err := s.promotionWorkflowClient.ResumePromotionWorkflow(ctx, &promotionsv1.ResumePromotionWorkflowRequest{
		Context: req.GetContext(),
		Promotion: &promotionsv1.Promotion{
			Owner: &identityv1.UserRef{
				Email:       req.GetProduct().GetOwner().GetEmail(),
				DisplayName: req.GetProduct().GetOwner().GetDisplayName(),
				UserId:      req.GetProduct().GetOwner().GetUserId(),
			},
			Description: req.GetProduct().GetComponents()["primary"].GetDescription(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionWorkflowService/ResumePromotionWorkflow")
	}
	return &catalogv1.CancelProductWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: scheduleBrandReportReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &catalogv1.CancelProductWorkflowResponse_Outcome{
			Status: resumePromotionWorkflowReply.GetResult().GetStatus(),
		},
		Product: &catalogv1.Product{
			ProductId: req.GetProductId(),
			Status:    req.GetProduct().GetStatus(),
		},
	}, nil
}

// RetryProductWorkflow retrys a product.
func (s *ProductWorkflowServer) RetryProductWorkflow(ctx context.Context, req *catalogv1.RetryProductWorkflowRequest) (*catalogv1.RetryProductWorkflowResponse, error) {
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "retry product workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "catalog.retry_product_workflow.share_brand_report",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"details.recipient.email":             req.GetProduct().GetOwner().GetEmail(),
			"details.recipient.phone":             req.GetDetails().GetNotify().GetPhone(),
			"details.recipient.preferred_channel": req.GetDetails().GetNotify().GetPreferredChannel(),
			"context.tenant_id":                   req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	compareSessionVersionsRequest := &identityv1.CompareSessionVersionsRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Assignee: &identityv1.UserRef{DisplayName: req.GetProduct().GetOwner().GetDisplayName()},
		},
	}
	compareSessionVersionsReply, err := s.sessionHistoryLazy().CompareSessionVersions(ctx, compareSessionVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionHistoryService/CompareSessionVersions")
	}
	previewLedgerAccountReply, err := s.ledgerAccountValidationClient.PreviewLedgerAccount(ctx, &ledgerv1.PreviewLedgerAccountRequest{
		Context: req.GetContext(),
		Details: &ledgerv1.PreviewLedgerAccountRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: compareSessionVersionsReply.GetSession().GetRequester().GetEmail(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/PreviewLedgerAccount")
	}
	return &catalogv1.RetryProductWorkflowResponse{
		Product: &catalogv1.Product{
			Audit: &commonv1.AuditInfo{
				Etag:      compareSessionVersionsReply.GetSession().GetAudit().GetEtag(),
				UpdatedAt: compareSessionVersionsReply.GetSession().GetAudit().GetUpdatedAt(),
			},
			ProductId: req.GetProductId(),
			Specs:     req.GetProduct().GetSpecs(),
		},
		Outcome: &catalogv1.RetryProductWorkflowResponse_Outcome{Message: previewLedgerAccountReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: previewLedgerAccountReply.GetResult().GetStatus()},
	}, nil
}

// PauseProductWorkflow pauses a product.
func (s *ProductWorkflowServer) PauseProductWorkflow(ctx context.Context, req *catalogv1.PauseProductWorkflowRequest) (*catalogv1.PauseProductWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "pause product workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	exportProductsStream, err := s.productQueryClient.ExportProducts(ctx, &catalogv1.ExportProductsRequest{
		Context: req.GetContext(),
		Filter:  &catalogv1.ExportProductsRequest_Filter{Region: req.GetProduct().GetAddress().GetRegion()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductQueryService/ExportProducts")
	}
	for {
		received, err := exportProductsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductQueryService/ExportProducts")
		}
		_ = received
	}
	subscriptionApproval, ok := s.clients.Get("acme.subscriptions.v1.SubscriptionApprovalService").(subscriptionsv1.SubscriptionApprovalServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.subscriptions.v1.SubscriptionApprovalService")
	}
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNotify().GetEmail())
	ctx = tenancy.With(ctx, "updated_by", req.GetProduct().GetAudit().GetUpdatedBy())
	recallSubscriptionRequest := &subscriptionsv1.RecallSubscriptionRequest{
		Context: req.GetContext(),
		Subscription: &subscriptionsv1.Subscription{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
			Audit:   &commonv1.AuditInfo{UpdatedBy: tenancy.Value(ctx, "updated_by")},
			Holder:  &customersv1.CustomerRef{DisplayName: req.GetProduct().GetOwner().GetDisplayName()},
		},
		Details: &subscriptionsv1.RecallSubscriptionRequest_Details{
			Approver: &identityv1.UserRef{Email: tenancy.Value(ctx, "email")},
		},
	}
	recallSubscriptionReply, err := subscriptionApproval.RecallSubscription(ctx, recallSubscriptionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionApprovalService/RecallSubscription")
	}
	return &catalogv1.PauseProductWorkflowResponse{
		Product: &catalogv1.Product{
			Audit: &commonv1.AuditInfo{
				Etag: recallSubscriptionReply.GetSubscription().GetAudit().GetEtag(),
			},
			ProductId: req.GetProductId(),
			Name:      req.GetProduct().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ResumeProductWorkflow resumes a product.
func (s *ProductWorkflowServer) ResumeProductWorkflow(ctx context.Context, req *catalogv1.ResumeProductWorkflowRequest) (*catalogv1.ResumeProductWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "resume product workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetProduct()).(*catalogv1.Product)
	lintUsersReply, err := s.userValidationClient.LintUsers(ctx, &identityv1.LintUsersRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			PrimaryAddress: &commonv1.PostalAddress{Region: req.GetProduct().GetAddress().GetRegion()},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetProduct().GetAudit().GetUpdatedBy(),
				CreatedBy: cloned.GetAudit().GetCreatedBy(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/LintUsers")
	}
	streamProductVariantMetricsStream, err := s.productVariantReportingClient.StreamProductVariantMetrics(ctx, &catalogv1.StreamProductVariantMetricsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email: req.GetProduct().GetOwner().GetEmail(),
			Phone: lintUsersReply.GetUser().GetContact().GetPhone(),
		},
		Filter: &catalogv1.StreamProductVariantMetricsRequest_Filter{
			Region: lintUsersReply.GetUser().GetPrimaryAddress().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantReportingService/StreamProductVariantMetrics")
	}
	for {
		received, err := streamProductVariantMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantReportingService/StreamProductVariantMetrics")
		}
		_ = received
	}
	packed, err := anypb.New(req.GetProduct())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	commentRedemptionApprovalRequest := &promotionsv1.CommentRedemptionApprovalRequest{
		Context: req.GetContext(),
		Redemption: &promotionsv1.Redemption{
			Subject: &promotionsv1.CouponRef{
				DisplayName: lintUsersReply.GetUser().GetRelationshipManager().GetDisplayName(),
			},
			Detail: packed,
		},
		Details: &promotionsv1.CommentRedemptionApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetProduct().GetOwner().GetDisplayName(),
				Email:       req.GetProduct().GetOwner().GetEmail(),
			},
		},
	}
	commentRedemptionApprovalReply, err := grpcx.NewClient(s.conn, promotionsv1.NewRedemptionApprovalServiceClient).CommentRedemptionApproval(ctx, commentRedemptionApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionApprovalService/CommentRedemptionApproval")
	}
	ctx = tenancy.With(ctx, "user_id", req.GetProduct().GetOwner().GetUserId())
	var countryCode string
	switch choice := req.GetProduct().GetLocator().(type) {
	case *catalogv1.Product_Address:
		countryCode = choice.Address.GetCountryCode()
	case *catalogv1.Product_FacilityCode:
		countryCode = choice.FacilityCode
	}
	escalateProductRequest := &catalogv1.EscalateProductRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Owner:   &identityv1.UserRef{UserId: tenancy.Value(ctx, "user_id")},
			Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{CountryCode: countryCode}},
		},
		Details: &catalogv1.EscalateProductRequest_Details{
			Approver: &identityv1.UserRef{DisplayName: req.GetProduct().GetOwner().GetDisplayName()},
		},
	}
	escalateProductReply, err := s.meteredProductApproval.EscalateProduct(ctx, escalateProductRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductApprovalService/EscalateProduct")
	}
	return &catalogv1.ResumeProductWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message: lintUsersReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Outcome: &catalogv1.ResumeProductWorkflowResponse_Outcome{
			NextSteps:   lintUsersReply.GetOutcome().GetNextSteps(),
			Status:      commentRedemptionApprovalReply.GetResult().GetStatus(),
			EffectiveAt: escalateProductReply.GetOutcome().GetEffectiveAt(),
		},
		Product: &catalogv1.Product{
			ProductId: req.GetProductId(),
			Code:      req.GetProduct().GetCode(),
		},
	}, nil
}

// InspectProductWorkflow returns a product.
func (s *ProductWorkflowServer) InspectProductWorkflow(ctx context.Context, req *catalogv1.InspectProductWorkflowRequest) (*catalogv1.InspectProductWorkflowResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product", req.GetProductId(), "inspect_product_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect product workflow", "tenant", req.GetContext().GetTenantId())
	onboardingPlanCalculation, err := registry.Lookup[onboardingv1.OnboardingPlanCalculationServiceClient](s.clients, "acme.onboarding.v1.OnboardingPlanCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	explainOnboardingPlanCalculationRequest := &onboardingv1.ExplainOnboardingPlanCalculationRequest{
		Context: req.GetContext(),
		Details: &onboardingv1.ExplainOnboardingPlanCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		OnboardingPlan: &onboardingv1.OnboardingPlan{
			Audit:    &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
			Metadata: req.GetContext().GetBaggage(),
		},
	}
	explainOnboardingPlanCalculationReply, err := onboardingPlanCalculation.ExplainOnboardingPlanCalculation(ctx, explainOnboardingPlanCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.onboarding.v1.OnboardingPlanCalculationService/ExplainOnboardingPlanCalculation")
	}
	snapshot, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	repairRoleRequest := &identityv1.RepairRoleRequest{
		Context: req.GetContext(),
		Role: &identityv1.Role{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: explainOnboardingPlanCalculationReply.GetOnboardingPlan().GetAudit().GetUpdatedAt(),
			},
		},
		Details: &identityv1.RepairRoleRequest_Details{Payload: snapshot},
	}
	repairRoleReply := new(identityv1.RepairRoleResponse)
	if err := s.conn.Invoke(ctx, identityRoleAdminServiceRepairRoleMethod, repairRoleRequest, repairRoleReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleAdminService/RepairRole")
	}
	return &catalogv1.InspectProductWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status: explainOnboardingPlanCalculationReply.GetOutcome().GetStatus(),
		},
		Product: &catalogv1.Product{
			Audit: &commonv1.AuditInfo{
				CreatedAt: repairRoleReply.GetRole().GetAudit().GetCreatedAt(),
			},
			ProductId: req.GetProductId(),
		},
	}, nil
}

// ListProductWorkflows lists products a page at a time.
func (s *ProductWorkflowServer) ListProductWorkflows(ctx context.Context, req *catalogv1.ListProductWorkflowsRequest) (*catalogv1.ListProductWorkflowsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list product workflows", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetFilter()).(*catalogv1.ListProductWorkflowsRequest_Filter)
	streamBrandMetricsStream, err := s.brandReportingClient.StreamBrandMetrics(ctx, &catalogv1.StreamBrandMetricsRequest{
		Context: req.GetContext(),
		Filter: &catalogv1.StreamBrandMetricsRequest_Filter{
			CreatedAfter: cloned.GetCreatedAfter(),
			OwnerId:      req.GetFilter().GetOwnerId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandReportingService/StreamBrandMetrics")
	}
	for {
		received, err := streamBrandMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandReportingService/StreamBrandMetrics")
		}
		_ = received
	}
	repairSearchIndexRequest := buildRepairSearchIndexRequestForListProductWorkflows(req)
	repairSearchIndexReply, err := s.searchIndexAdminClient.RepairSearchIndex(ctx, repairSearchIndexRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexAdminService/RepairSearchIndex")
	}
	return &catalogv1.ListProductWorkflowsResponse{
		Products: []*catalogv1.Product{&catalogv1.Product{
			Audit: &commonv1.AuditInfo{
				CreatedBy: repairSearchIndexReply.GetSearchIndex().GetAudit().GetCreatedBy(),
				UpdatedBy: repairSearchIndexReply.GetSearchIndex().GetAudit().GetUpdatedBy(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SignalProductWorkflow signals a product.
func (s *ProductWorkflowServer) SignalProductWorkflow(ctx context.Context, req *catalogv1.SignalProductWorkflowRequest) (*catalogv1.SignalProductWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "signal product workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	batchGetDispatchOrdersRequest := &dispatchv1.BatchGetDispatchOrdersRequest{Context: req.GetContext()}
	batchGetDispatchOrdersReply, err := s.meteredDispatchOrder.BatchGetDispatchOrders(ctx, batchGetDispatchOrdersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.dispatch.v1.DispatchOrderService/BatchGetDispatchOrders")
	}
	rejectCategoryRequest := &catalogv1.RejectCategoryRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Audit: &commonv1.AuditInfo{
						UpdatedAt: req.GetProduct().GetAudit().GetUpdatedAt(),
						UpdatedBy: req.GetProduct().GetAudit().GetUpdatedBy(),
					},
				}},
				Manager: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
			}},
		},
	}
	rejectCategoryReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*catalogv1.RejectCategoryResponse, error) {
		return s.categoryApprovalClient.RejectCategory(ctx, rejectCategoryRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryApprovalService/RejectCategory")
	}
	findProductsRequest := &catalogv1.FindProductsRequest{
		Context: req.GetContext(),
		Filter: &catalogv1.FindProductsRequest_Filter{
			Region:       rejectCategoryReply.GetCategory().GetLocation().GetRegion(),
			CreatedAfter: rejectCategoryReply.GetCategory().GetAudit().GetCreatedAt(),
		},
	}
	findProductsReply, err := grpcx.Call(ctx, s.productQueryClient.FindProducts, findProductsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductQueryService/FindProducts")
	}
	snapshot, err := protojson.Marshal(req.GetProduct())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pushAttributeRequest := &catalogv1.PushAttributeRequest{
		Context: req.GetContext(),
		Attribute: &catalogv1.Attribute{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetProduct().GetAudit().GetUpdatedBy(),
				UpdatedAt: req.GetProduct().GetAudit().GetUpdatedAt(),
			},
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
		},
		Details: &catalogv1.PushAttributeRequest_Details{Payload: snapshot},
	}
	pushAttributeReply := new(catalogv1.PushAttributeResponse)
	if err := grpcx.Invoke(ctx, s.conn, catalogv1.AttributeSyncService_PushAttribute_FullMethodName, pushAttributeRequest, pushAttributeReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.AttributeSyncService/PushAttribute")
	}
	return &catalogv1.SignalProductWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message:       batchGetDispatchOrdersReply.GetResult().GetMessage(),
			CorrelationId: rejectCategoryReply.GetResult().GetCorrelationId(),
			Status:        rejectCategoryReply.GetOutcome().GetStatus(),
			Details:       findProductsReply.GetResult().GetDetails(),
		},
		Outcome: &catalogv1.SignalProductWorkflowResponse_Outcome{Status: findProductsReply.GetResult().GetStatus()},
		Product: &catalogv1.Product{
			Owner: &identityv1.UserRef{
				DisplayName: pushAttributeReply.GetAttribute().GetOwner().GetDisplayName(),
			},
			ProductId: req.GetProductId(),
			Specs:     req.GetProduct().GetSpecs(),
		},
	}, nil
}

// redemptionApprovalLazy dials acme.promotions.v1.RedemptionApprovalService the first time a handler needs it.
func (s *ProductWorkflowServer) redemptionApprovalLazy() promotionsv1.RedemptionApprovalServiceClient {
	s.redemptionApprovalOnce.Do(func() { s.redemptionApprovalCached = promotionsv1.NewRedemptionApprovalServiceClient(s.conn) })
	return s.redemptionApprovalCached
}

// sessionHistoryLazy dials acme.identity.v1.SessionHistoryService the first time a handler needs it.
func (s *ProductWorkflowServer) sessionHistoryLazy() identityv1.SessionHistoryServiceClient {
	s.sessionHistoryOnce.Do(func() { s.sessionHistoryCached = identityv1.NewSessionHistoryServiceClient(s.conn) })
	return s.sessionHistoryCached
}
