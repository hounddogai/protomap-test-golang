package catalog

import (
	"context"
	"log/slog"
	"sync"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// CategoryWorkflowServer implements `acme.catalog.v1.CategoryWorkflowService`.
// Runs the long-lived workflow that takes a category from start to finish.
type CategoryWorkflowServer struct {
	catalogv1.UnimplementedCategoryWorkflowServiceServer
	store                           *Store
	log                             *slog.Logger
	cartWorkflowClient              cartv1.CartWorkflowServiceClient
	searchQueryValidationClient     searchv1.SearchQueryValidationServiceClient
	pointsLedgerApprovalClient      loyaltyv1.PointsLedgerApprovalServiceClient
	brandReportingClient            catalogv1.BrandReportingServiceClient
	conn                            *grpc.ClientConn
	brandReportingOnce              sync.Once
	brandReportingCached            catalogv1.BrandReportingServiceClient
	meteredUserQuery                meteredUserQuery
	productApprovalPort             productApprovalPort
	paymentInstrumentApprovalPort   paymentInstrumentApprovalPort
	paymentInstrumentApprovalClient paymentsv1.PaymentInstrumentApprovalServiceClient
	fulfillmentWorkflowPort         fulfillmentWorkflowPort
	clients                         *registry.Registry
	bus                             *eventbus.Bus
}

// NewCategoryWorkflowServer wires a CategoryWorkflowServer.
func NewCategoryWorkflowServer(deps platform.Deps) *CategoryWorkflowServer {
	return &CategoryWorkflowServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.catalog.v1.CategoryWorkflowService"),
		cartWorkflowClient:              cartv1.NewCartWorkflowServiceClient(deps.Conn),
		searchQueryValidationClient:     searchv1.NewSearchQueryValidationServiceClient(deps.Conn),
		pointsLedgerApprovalClient:      loyaltyv1.NewPointsLedgerApprovalServiceClient(deps.Conn),
		brandReportingClient:            catalogv1.NewBrandReportingServiceClient(deps.Conn),
		conn:                            deps.Conn,
		meteredUserQuery:                meteredUserQuery{UserQueryServiceClient: identityv1.NewUserQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		productApprovalPort:             newProductApprovalPort(deps),
		paymentInstrumentApprovalPort:   newPaymentInstrumentApprovalPort(deps),
		paymentInstrumentApprovalClient: paymentsv1.NewPaymentInstrumentApprovalServiceClient(deps.Conn),
		fulfillmentWorkflowPort:         newFulfillmentWorkflowPort(deps),
		clients:                         deps.Clients,
		bus:                             deps.Bus,
	}
}

// StartCategoryWorkflow starts a category.
func (s *CategoryWorkflowServer) StartCategoryWorkflow(ctx context.Context, req *catalogv1.StartCategoryWorkflowRequest) (*catalogv1.StartCategoryWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected StartCategoryWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveCategory(ctx, req.GetContext().GetTenantId(), req.GetCategory()); err != nil {
		return nil, grpcx.Wrap(err, "save category")
	}
	s.log.InfoContext(ctx, "start category workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCategory().GetAudit().GetEtag())
	source := req.GetCategory().GetLocation().ProtoReflect()
	region := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("region"))).String()
	cloned := proto.Clone(req.GetCategory()).(*catalogv1.Category)
	listCartWorkflowsReply, err := s.cartWorkflowClient.ListCartWorkflows(ctx, &cartv1.ListCartWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &cartv1.ListCartWorkflowsRequest_Filter{
			Region:       region,
			CreatedAfter: cloned.GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetCategory().GetManager().GetUserId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartWorkflowService/ListCartWorkflows")
	}
	normalizeSearchQueryRequest := &searchv1.NormalizeSearchQueryRequest{
		Context: req.GetContext(),
		Details: &searchv1.NormalizeSearchQueryRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
		},
	}
	var normalizeSearchQueryReply *searchv1.NormalizeSearchQueryResponse
	if err := pipeline.Run(ctx, "acme.search.v1.SearchQueryValidationService/NormalizeSearchQuery", func(ctx context.Context) error {
		answer, err := s.searchQueryValidationClient.NormalizeSearchQuery(ctx, normalizeSearchQueryRequest)
		normalizeSearchQueryReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryValidationService/NormalizeSearchQuery")
	}
	listPendingPointsLedgersRequest := &loyaltyv1.ListPendingPointsLedgersRequest{
		Context: req.GetContext(),
		Filter: &loyaltyv1.ListPendingPointsLedgersRequest_Filter{
			Region:       req.GetCategory().GetLocation().GetRegion(),
			CreatedAfter: req.GetCategory().GetAudit().GetUpdatedAt(),
		},
	}
	listPendingPointsLedgersReply, err := grpcx.Call(ctx, s.pointsLedgerApprovalClient.ListPendingPointsLedgers, listPendingPointsLedgersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerApprovalService/ListPendingPointsLedgers")
	}
	ctx = tenancy.With(ctx, "user_id", req.GetCategory().GetManager().GetUserId())
	countBrandTotalsRequest := &catalogv1.CountBrandTotalsRequest{
		Context: req.GetContext(),
		Filter: &catalogv1.CountBrandTotalsRequest_Filter{
			Region:       req.GetCategory().GetLocation().GetRegion(),
			CreatedAfter: req.GetCategory().GetAudit().GetUpdatedAt(),
			OwnerId:      tenancy.Value(ctx, "user_id"),
		},
	}
	countBrandTotalsReply, err := catalogv1.BrandReportingServiceClient.CountBrandTotals(s.brandReportingClient, ctx, countBrandTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandReportingService/CountBrandTotals")
	}
	return &catalogv1.StartCategoryWorkflowResponse{
		Outcome: &catalogv1.StartCategoryWorkflowResponse_Outcome{Status: listCartWorkflowsReply.GetResult().GetStatus()},
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Attributes: normalizeSearchQueryReply.GetSearchQuery().GetAttributes(),
			}},
			CategoryId: req.GetCategoryId(),
			Manager:    req.GetCategory().GetManager(),
		},
		Result: &commonv1.OperationResult{
			Details:       listPendingPointsLedgersReply.GetResult().GetDetails(),
			CorrelationId: countBrandTotalsReply.GetResult().GetCorrelationId(),
			Message:       countBrandTotalsReply.GetResult().GetMessage(),
			Status:        "ok",
		},
	}, nil
}

// AdvanceCategoryWorkflow advances a category.
func (s *CategoryWorkflowServer) AdvanceCategoryWorkflow(ctx context.Context, req *catalogv1.AdvanceCategoryWorkflowRequest) (*catalogv1.AdvanceCategoryWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCategory(ctx, req.GetContext().GetTenantId(), req.GetCategory()); err != nil {
		return nil, grpcx.Wrap(err, "save category")
	}
	s.log.InfoContext(ctx, "advance category workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCategory().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetCategory())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	shareBrandReportRequest := &catalogv1.ShareBrandReportRequest{
		Context: req.GetContext(),
		Details: &catalogv1.ShareBrandReportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				Name:             req.GetDetails().GetNotify().GetName(),
				Email:            req.GetDetails().GetNotify().GetEmail(),
			},
			Payload: packed,
		},
	}
	shareBrandReportReply, err := s.brandReportingLazy().ShareBrandReport(ctx, shareBrandReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandReportingService/ShareBrandReport")
	}
	findUsersRequest := &identityv1.FindUsersRequest{
		Context: req.GetContext(),
		Filter: &identityv1.FindUsersRequest_Filter{
			Region:       shareBrandReportReply.GetBrand().GetPrimaryAddress().GetRegion(),
			CreatedAfter: shareBrandReportReply.GetBrand().GetAudit().GetUpdatedAt(),
			OwnerId:      shareBrandReportReply.GetResult().GetCorrelationId(),
		},
	}
	findUsersReply, err := s.meteredUserQuery.FindUsers(ctx, findUsersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/FindUsers")
	}
	return &catalogv1.AdvanceCategoryWorkflowResponse{
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Location: &commonv1.PostalAddress{
						CountryCode: shareBrandReportReply.GetBrand().GetPrimaryAddress().GetCountryCode(),
					},
				}},
			}},
			Location: &commonv1.PostalAddress{
				Region: shareBrandReportReply.GetBrand().GetPrimaryAddress().GetRegion(),
			},
			CategoryId: req.GetCategoryId(),
			Status:     req.GetCategory().GetStatus(),
		},
		Outcome: &catalogv1.AdvanceCategoryWorkflowResponse_Outcome{Status: findUsersReply.GetResult().GetStatus()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CancelCategoryWorkflow cancels a category.
func (s *CategoryWorkflowServer) CancelCategoryWorkflow(ctx context.Context, req *catalogv1.CancelCategoryWorkflowRequest) (*catalogv1.CancelCategoryWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCategory(ctx, req.GetContext().GetTenantId(), req.GetCategory()); err != nil {
		return nil, grpcx.Wrap(err, "save category")
	}
	s.log.InfoContext(ctx, "cancel category workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCategory().GetAudit().GetEtag())
	recallProductRequest := &catalogv1.RecallProductRequest{
		Context: req.GetContext(),
		Details: &catalogv1.RecallProductRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
		},
	}
	recallProductReply, err := s.productApprovalPort.RecallProduct(ctx, recallProductRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductApprovalService/RecallProduct")
	}
	names := collectCategoryNames(req.GetCategory(), nil)
	recallPaymentInstrumentRequest := &paymentsv1.RecallPaymentInstrumentRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Contact: &commonv1.ContactInfo{Email: req.GetCategory().GetManager().GetEmail()},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: recallProductReply.GetProduct().GetAudit().GetUpdatedBy(),
			},
		},
		Details: &paymentsv1.RecallPaymentInstrumentRequest_Details{Conditions: names},
	}
	recallPaymentInstrumentReply, err := s.paymentInstrumentApprovalPort.RecallPaymentInstrument(ctx, recallPaymentInstrumentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentApprovalService/RecallPaymentInstrument")
	}
	return &catalogv1.CancelCategoryWorkflowResponse{
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Audit: &commonv1.AuditInfo{
					UpdatedAt: recallProductReply.GetProduct().GetAudit().GetUpdatedAt(),
				},
				Children: []*catalogv1.Category{&catalogv1.Category{
					Manager: &identityv1.UserRef{
						Email: recallPaymentInstrumentReply.GetPaymentInstrument().GetContact().GetEmail(),
					},
				}},
			}},
			CategoryId: req.GetCategoryId(),
			Attributes: req.GetCategory().GetAttributes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PauseCategoryWorkflow pauses a category.
func (s *CategoryWorkflowServer) PauseCategoryWorkflow(ctx context.Context, req *catalogv1.PauseCategoryWorkflowRequest) (*catalogv1.PauseCategoryWorkflowResponse, error) {
	if err := s.store.SaveCategory(ctx, req.GetContext().GetTenantId(), req.GetCategory()); err != nil {
		return nil, grpcx.Wrap(err, "save category")
	}
	s.log.InfoContext(ctx, "pause category workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCategory().GetAudit().GetEtag())
	source := req.GetCategory().GetManager().ProtoReflect()
	userId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("user_id"))).String()
	ctx = tenancy.With(ctx, "etag", req.GetCategory().GetAudit().GetEtag())
	submitPaymentInstrumentRequest := &paymentsv1.SubmitPaymentInstrumentRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.SubmitPaymentInstrumentRequest_Details{Approver: &identityv1.UserRef{UserId: userId}},
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Audit: &commonv1.AuditInfo{
				Etag:      tenancy.Value(ctx, "etag"),
				UpdatedAt: req.GetCategory().GetAudit().GetUpdatedAt(),
			},
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNotify().GetEmail()},
		},
	}
	submitPaymentInstrumentReply, err := grpcx.Call(ctx, s.paymentInstrumentApprovalClient.SubmitPaymentInstrument, submitPaymentInstrumentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentApprovalService/SubmitPaymentInstrument")
	}
	return &catalogv1.PauseCategoryWorkflowResponse{
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Audit: &commonv1.AuditInfo{
					UpdatedBy: submitPaymentInstrumentReply.GetPaymentInstrument().GetAudit().GetUpdatedBy(),
				},
			}},
			CategoryId: req.GetCategoryId(),
			Name:       req.GetCategory().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListCategoryWorkflows lists categories a page at a time.
func (s *CategoryWorkflowServer) ListCategoryWorkflows(ctx context.Context, req *catalogv1.ListCategoryWorkflowsRequest) (*catalogv1.ListCategoryWorkflowsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list category workflows", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "region", req.GetFilter().GetRegion())
	cancelFulfillmentWorkflowRequest := &fulfillmentv1.CancelFulfillmentWorkflowRequest{
		Context: req.GetContext(),
		Fulfillment: &fulfillmentv1.Fulfillment{
			BillingAddress: &commonv1.PostalAddress{Region: tenancy.Value(ctx, "region")},
		},
		Details: &fulfillmentv1.CancelFulfillmentWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	cancelFulfillmentWorkflowReply, err := s.fulfillmentWorkflowPort.CancelFulfillmentWorkflow(ctx, cancelFulfillmentWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentWorkflowService/CancelFulfillmentWorkflow")
	}
	return &catalogv1.ListCategoryWorkflowsResponse{
		Categories: []*catalogv1.Category{&catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Manager: &identityv1.UserRef{
						DisplayName: cancelFulfillmentWorkflowReply.GetFulfillment().GetCounterparty().GetDisplayName(),
					},
					Audit: &commonv1.AuditInfo{
						UpdatedBy: cancelFulfillmentWorkflowReply.GetFulfillment().GetAudit().GetUpdatedBy(),
					},
				}},
			}},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SignalCategoryWorkflow signals a category.
func (s *CategoryWorkflowServer) SignalCategoryWorkflow(ctx context.Context, req *catalogv1.SignalCategoryWorkflowRequest) (*catalogv1.SignalCategoryWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCategory(ctx, req.GetContext().GetTenantId(), req.GetCategory()); err != nil {
		return nil, grpcx.Wrap(err, "save category")
	}
	s.log.InfoContext(ctx, "signal category workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCategory().GetAudit().GetEtag())
	featureFlagQuery, ok := s.clients.Get("acme.featureflags.v1.FeatureFlagQueryService").(featureflagsv1.FeatureFlagQueryServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.featureflags.v1.FeatureFlagQueryService")
	}
	suggestFeatureFlagsRequest := &featureflagsv1.SuggestFeatureFlagsRequest{
		Context: req.GetContext(),
		Filter:  &featureflagsv1.SuggestFeatureFlagsRequest_Filter{Region: req.GetCategory().GetLocation().GetRegion()},
	}
	suggestFeatureFlagsReply, err := featureFlagQuery.SuggestFeatureFlags(ctx, suggestFeatureFlagsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/SuggestFeatureFlags")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "catalog.signal_category_workflow.escalate_payment_instrument",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"details.approver.user_id":         req.GetCategory().GetManager().GetUserId(),
			"payment_instrument.contact.phone": req.GetDetails().GetNotify().GetPhone(),
			"details.approver.email":           req.GetCategory().GetManager().GetEmail(),
			"context.tenant_id":                req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	countBrandTotalsRequest := &catalogv1.CountBrandTotalsRequest{
		Context: req.GetContext(),
		Filter:  &catalogv1.CountBrandTotalsRequest_Filter{Region: req.GetCategory().GetLocation().GetRegion()},
	}
	countBrandTotalsReply, err := catalogv1.BrandReportingServiceClient.CountBrandTotals(s.brandReportingClient, ctx, countBrandTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandReportingService/CountBrandTotals")
	}
	return &catalogv1.SignalCategoryWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message:       suggestFeatureFlagsReply.GetResult().GetMessage(),
			CorrelationId: countBrandTotalsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &catalogv1.SignalCategoryWorkflowResponse_Outcome{Message: countBrandTotalsReply.GetResult().GetMessage()},
		Category: &catalogv1.Category{
			CategoryId: req.GetCategoryId(),
			Attributes: req.GetCategory().GetAttributes(),
		},
	}, nil
}

// brandReportingLazy dials acme.catalog.v1.BrandReportingService the first time a handler needs it.
func (s *CategoryWorkflowServer) brandReportingLazy() catalogv1.BrandReportingServiceClient {
	s.brandReportingOnce.Do(func() { s.brandReportingCached = catalogv1.NewBrandReportingServiceClient(s.conn) })
	return s.brandReportingCached
}
