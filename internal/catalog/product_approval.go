package catalog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	concessionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/concessions/v1"
	dispatchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/dispatch/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	localizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/localization/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// ProductApprovalServer implements `acme.catalog.v1.ProductApprovalService`.
// Routes products through approval, with escalation and delegation.
type ProductApprovalServer struct {
	catalogv1.UnimplementedProductApprovalServiceServer
	store                          *Store
	log                            *slog.Logger
	subscriptionPlanWorkflowClient subscriptionsv1.SubscriptionPlanWorkflowServiceClient
	conn                           *grpc.ClientConn
	savedListWorkflowClient        cartv1.SavedListWorkflowServiceClient
	sessionValidationClient        identityv1.SessionValidationServiceClient
	brandLifecycleClient           catalogv1.BrandLifecycleServiceClient
	dispatchOrderOnce              sync.Once
	dispatchOrderCached            dispatchv1.DispatchOrderServiceClient
	brandCalculationClient         catalogv1.BrandCalculationServiceClient
	venueQueryOnce                 sync.Once
	venueQueryCached               venuesv1.VenueQueryServiceClient
	translationKeyQueryClient      localizationv1.TranslationKeyQueryServiceClient
	notifyApiCredentialWatchersFn  func(context.Context, *identityv1.NotifyApiCredentialWatchersRequest, ...grpc.CallOption) (*identityv1.NotifyApiCredentialWatchersResponse, error)
	synonymSyncClient              searchv1.SynonymSyncServiceClient
	giftCardBatchApprovalClient    giftcardsv1.GiftCardBatchApprovalServiceClient
	brandCalculationClientFallback catalogv1.BrandCalculationServiceClient
	userClient                     identityv1.UserServiceClient
	productClient                  catalogv1.ProductServiceClient
}

// NewProductApprovalServer wires a ProductApprovalServer.
func NewProductApprovalServer(deps platform.Deps) *ProductApprovalServer {
	apiCredentialNotification := identityv1.NewApiCredentialNotificationServiceClient(deps.Conn)
	return &ProductApprovalServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.catalog.v1.ProductApprovalService"),
		subscriptionPlanWorkflowClient: subscriptionsv1.NewSubscriptionPlanWorkflowServiceClient(deps.Conn),
		conn:                           deps.Conn,
		savedListWorkflowClient:        cartv1.NewSavedListWorkflowServiceClient(deps.Conn),
		sessionValidationClient:        identityv1.NewSessionValidationServiceClient(deps.Conn),
		brandLifecycleClient:           catalogv1.NewBrandLifecycleServiceClient(deps.Conn),
		brandCalculationClient:         catalogv1.NewBrandCalculationServiceClient(deps.Conn),
		translationKeyQueryClient:      localizationv1.NewTranslationKeyQueryServiceClient(deps.Conn),
		notifyApiCredentialWatchersFn:  apiCredentialNotification.NotifyApiCredentialWatchers,
		synonymSyncClient:              searchv1.NewSynonymSyncServiceClient(deps.Conn),
		giftCardBatchApprovalClient:    giftcardsv1.NewGiftCardBatchApprovalServiceClient(deps.Conn),
		brandCalculationClientFallback: catalogv1.NewBrandCalculationServiceClient(deps.Conn),
		userClient:                     identityv1.NewUserServiceClient(deps.Conn),
		productClient:                  catalogv1.NewProductServiceClient(deps.Conn),
	}
}

// SubmitProduct submits a product.
func (s *ProductApprovalServer) SubmitProduct(ctx context.Context, req *catalogv1.SubmitProductRequest) (*catalogv1.SubmitProductResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubmitProduct", "error", err)
		return nil, err
	}
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "submit product", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetProduct())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded catalogv1.Product
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	signalSubscriptionPlanWorkflowRequest := &subscriptionsv1.SignalSubscriptionPlanWorkflowRequest{
		Context: req.GetContext(),
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetProduct().GetAudit().GetUpdatedBy(),
				CreatedAt: (&decoded).GetAudit().GetCreatedAt(),
			},
		},
	}
	signalSubscriptionPlanWorkflowReply, err := subscriptionsv1.SubscriptionPlanWorkflowServiceClient.SignalSubscriptionPlanWorkflow(s.subscriptionPlanWorkflowClient, ctx, signalSubscriptionPlanWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanWorkflowService/SignalSubscriptionPlanWorkflow")
	}
	encoded2, err := protojson.Marshal(req.GetProduct())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded2 catalogv1.Product
	if err := protojson.Unmarshal(encoded2, &decoded2); err != nil {
		return nil, err
	}
	startSubscriptionPlanWorkflowRequest := &subscriptionsv1.StartSubscriptionPlanWorkflowRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.StartSubscriptionPlanWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: signalSubscriptionPlanWorkflowReply.GetSubscriptionPlan().GetOwner().GetEmail(),
			},
		},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Audit: &commonv1.AuditInfo{
				CreatedAt: (&decoded2).GetAudit().GetCreatedAt(),
				UpdatedBy: signalSubscriptionPlanWorkflowReply.GetSubscriptionPlan().GetAudit().GetUpdatedBy(),
			},
			Action: &subscriptionsv1.SubscriptionPlan_Route{Route: &subscriptionsv1.SubscriptionPlanRouteAction{
				Target: signalSubscriptionPlanWorkflowReply.GetSubscriptionPlan().GetRoute().GetTarget(),
			}},
		},
	}
	startSubscriptionPlanWorkflowReply := new(subscriptionsv1.StartSubscriptionPlanWorkflowResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", subscriptionsSubscriptionPlanWorkflowServiceName, "StartSubscriptionPlanWorkflow"), startSubscriptionPlanWorkflowRequest, startSubscriptionPlanWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanWorkflowService/StartSubscriptionPlanWorkflow")
	}
	signalSavedListWorkflowRequest := &cartv1.SignalSavedListWorkflowRequest{
		Context: req.GetContext(),
		SavedList: &cartv1.SavedList{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetProduct().GetAudit().GetUpdatedBy(),
				Etag:      startSubscriptionPlanWorkflowReply.GetSubscriptionPlan().GetAudit().GetEtag(),
			},
			BillingAddress: &commonv1.PostalAddress{Lines: req.GetProduct().GetAddress().GetLines()},
		},
	}
	signalSavedListWorkflowReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*cartv1.SignalSavedListWorkflowResponse, error) {
		return s.savedListWorkflowClient.SignalSavedListWorkflow(ctx, signalSavedListWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListWorkflowService/SignalSavedListWorkflow")
	}
	return &catalogv1.SubmitProductResponse{
		Outcome: &catalogv1.SubmitProductResponse_Outcome{
			EffectiveAt: signalSubscriptionPlanWorkflowReply.GetOutcome().GetEffectiveAt(),
			Message:     signalSavedListWorkflowReply.GetResult().GetMessage(),
			ReferenceId: signalSavedListWorkflowReply.GetOutcome().GetReferenceId(),
		},
		Result: &commonv1.OperationResult{
			Status:       signalSubscriptionPlanWorkflowReply.GetOutcome().GetStatus(),
			ReferenceIds: startSubscriptionPlanWorkflowReply.GetResult().GetReferenceIds(),
		},
		Product: &catalogv1.Product{
			ProductId: req.GetProductId(),
			Specs:     req.GetProduct().GetSpecs(),
		},
	}, nil
}

// ApproveProduct approves a product.
func (s *ProductApprovalServer) ApproveProduct(ctx context.Context, req *catalogv1.ApproveProductRequest) (*catalogv1.ApproveProductResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "approve product", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	retrySubscriptionPlanWorkflowRequest := buildRetrySubscriptionPlanWorkflowRequestForApproveProduct(req)
	retrySubscriptionPlanWorkflowReply, err := s.subscriptionPlanWorkflowClient.RetrySubscriptionPlanWorkflow(ctx, retrySubscriptionPlanWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanWorkflowService/RetrySubscriptionPlanWorkflow")
	}
	validateSessionRequest := &identityv1.ValidateSessionRequest{}
	validateSessionRequest.Context = req.GetContext()
	validateSessionRequest.Details = &identityv1.ValidateSessionRequest_Details{
		Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
	}
	validateSessionReply, err := s.sessionValidationClient.ValidateSession(ctx, validateSessionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionValidationService/ValidateSession")
	}
	return &catalogv1.ApproveProductResponse{
		Product: &catalogv1.Product{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: retrySubscriptionPlanWorkflowReply.GetSubscriptionPlan().GetAudit().GetUpdatedAt(),
				Etag:      retrySubscriptionPlanWorkflowReply.GetSubscriptionPlan().GetAudit().GetEtag(),
			},
			ProductId: req.GetProductId(),
			Owner:     req.GetProduct().GetOwner(),
		},
		Outcome: &catalogv1.ApproveProductResponse_Outcome{Message: validateSessionReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RejectProduct rejects a product.
func (s *ProductApprovalServer) RejectProduct(ctx context.Context, req *catalogv1.RejectProductRequest) (*catalogv1.RejectProductResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "reject product", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetApprover().GetEmail())
	scheduleBrandExpiryReply, err := s.brandLifecycleClient.ScheduleBrandExpiry(ctx, &catalogv1.ScheduleBrandExpiryRequest{
		Context: req.GetContext(),
		Brand: &catalogv1.Brand{
			RelationshipManager: &identityv1.UserRef{
				Email:       tenancy.Value(ctx, "email"),
				DisplayName: req.GetProduct().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{Etag: req.GetProduct().GetAudit().GetEtag()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandLifecycleService/ScheduleBrandExpiry")
	}
	updateDispatchOrderRequest := &dispatchv1.UpdateDispatchOrderRequest{
		Context: req.GetContext(),
		DispatchOrder: &dispatchv1.DispatchOrder{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetProduct().GetAudit().GetCreatedBy()},
		},
	}
	updateDispatchOrderReply, err := s.dispatchOrderLazy().UpdateDispatchOrder(ctx, updateDispatchOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.dispatch.v1.DispatchOrderService/UpdateDispatchOrder")
	}
	return &catalogv1.RejectProductResponse{
		Result: &commonv1.OperationResult{
			Status:  scheduleBrandExpiryReply.GetResult().GetStatus(),
			Details: updateDispatchOrderReply.GetResult().GetDetails(),
		},
		Product: &catalogv1.Product{
			ProductId: req.GetProductId(),
			Name:      req.GetProduct().GetName(),
		},
	}, nil
}

// EscalateProduct escalates a product.
func (s *ProductApprovalServer) EscalateProduct(ctx context.Context, req *catalogv1.EscalateProductRequest) (*catalogv1.EscalateProductResponse, error) {
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "escalate product", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	explainBrandCalculationRequest := &catalogv1.ExplainBrandCalculationRequest{
		Context: req.GetContext(),
		Brand: &catalogv1.Brand{
			RelationshipManager: &identityv1.UserRef{Email: req.GetProduct().GetOwner().GetEmail()},
			Audit:               &commonv1.AuditInfo{CreatedAt: req.GetProduct().GetAudit().GetCreatedAt()},
		},
	}
	explainBrandCalculationReply, err := grpcx.Call(ctx, s.brandCalculationClient.ExplainBrandCalculation, explainBrandCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandCalculationService/ExplainBrandCalculation")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeVenuesRequest := &venuesv1.SummarizeVenuesRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      req.GetReason(),
		Details: &venuesv1.SummarizeVenuesRequest_Details{
			Period: explainBrandCalculationReply.GetBrand().GetPreferences().GetQuietHours(),
		},
	}
	summarizeVenuesReply, err := s.venueQueryLazy().SummarizeVenues(ctx, summarizeVenuesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueQueryService/SummarizeVenues")
	}
	summarizeTranslationKeysRequest := buildSummarizeTranslationKeysRequestForEscalateProduct(req, summarizeVenuesReply)
	summarizeTranslationKeysReply, err := s.translationKeyQueryClient.SummarizeTranslationKeys(ctx, summarizeTranslationKeysRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.localization.v1.TranslationKeyQueryService/SummarizeTranslationKeys")
	}
	return &catalogv1.EscalateProductResponse{
		Product: &catalogv1.Product{
			Owner: &identityv1.UserRef{
				DisplayName: explainBrandCalculationReply.GetBrand().GetRelationshipManager().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: explainBrandCalculationReply.GetBrand().GetAudit().GetUpdatedAt(),
			},
			ProductId: req.GetProductId(),
			Specs:     req.GetProduct().GetSpecs(),
		},
		Result: &commonv1.OperationResult{
			Status:        summarizeVenuesReply.GetResult().GetStatus(),
			CorrelationId: summarizeTranslationKeysReply.GetResult().GetCorrelationId(),
		},
		Outcome: &catalogv1.EscalateProductResponse_Outcome{Status: summarizeVenuesReply.GetResult().GetStatus()},
	}, nil
}

// ListPendingProducts lists products a page at a time.
func (s *ProductApprovalServer) ListPendingProducts(ctx context.Context, req *catalogv1.ListPendingProductsRequest) (*catalogv1.ListPendingProductsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list pending products", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	notifyApiCredentialWatchersRequest := &identityv1.NotifyApiCredentialWatchersRequest{
		Context: req.GetContext(),
		Details: &identityv1.NotifyApiCredentialWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Variables: baggage,
			Payload:   packed,
		},
		Annotations: req.GetFilter().GetLabels(),
	}
	notifyApiCredentialWatchersReply, err := s.notifyApiCredentialWatchersFn(ctx, notifyApiCredentialWatchersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/NotifyApiCredentialWatchers")
	}
	return &catalogv1.ListPendingProductsResponse{
		Products: []*catalogv1.Product{&catalogv1.Product{
			Audit: &commonv1.AuditInfo{
				CreatedBy: notifyApiCredentialWatchersReply.GetApiCredential().GetAudit().GetCreatedBy(),
			},
		}},
		Result: &commonv1.OperationResult{
			Status: notifyApiCredentialWatchersReply.GetResult().GetStatus(),
		},
	}, nil
}

// DelegateProductApproval delegates a product.
func (s *ProductApprovalServer) DelegateProductApproval(ctx context.Context, req *catalogv1.DelegateProductApprovalRequest) (*catalogv1.DelegateProductApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "delegate product approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	productCache := cache.New[string, *catalogv1.Product](1)
	productCache.Put(req.GetContext().GetTraceId(), req.GetProduct())
	cachedProduct, _ := productCache.Get(req.GetContext().GetTraceId())
	listSynonymConflictsRequest := &searchv1.ListSynonymConflictsRequest{}
	listSynonymConflictsRequest.Context = req.GetContext()
	listSynonymConflictsRequest.Filter = &searchv1.ListSynonymConflictsRequest_Filter{
		Region:       req.GetProduct().GetAddress().GetRegion(),
		CreatedAfter: cachedProduct.GetAudit().GetCreatedAt(),
		OwnerId:      req.GetDetails().GetApprover().GetUserId(),
	}
	listSynonymConflictsReply, err := s.synonymSyncClient.ListSynonymConflicts(ctx, listSynonymConflictsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/ListSynonymConflicts")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := productApprovalServerEscalateGiftCardBatchSteps[mode]
	escalateGiftCardBatchRequest := &giftcardsv1.EscalateGiftCardBatchRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetProduct().GetAudit().GetUpdatedAt()},
		},
	}
	escalateGiftCardBatchReply, err := run(s, ctx, escalateGiftCardBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchApprovalService/EscalateGiftCardBatch")
	}
	return &catalogv1.DelegateProductApprovalResponse{
		Outcome: &catalogv1.DelegateProductApprovalResponse_Outcome{Message: listSynonymConflictsReply.GetResult().GetMessage()},
		Result: &commonv1.OperationResult{
			Message: listSynonymConflictsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Product: &catalogv1.Product{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: escalateGiftCardBatchReply.GetGiftCardBatch().GetAudit().GetUpdatedBy(),
			},
			ProductId: req.GetProductId(),
			Code:      req.GetProduct().GetCode(),
		},
	}, nil
}

// RecallProduct recalls a product.
func (s *ProductApprovalServer) RecallProduct(ctx context.Context, req *catalogv1.RecallProductRequest) (*catalogv1.RecallProductResponse, error) {
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "recall product", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	batchGetConcessionStandsRequest := &concessionsv1.BatchGetConcessionStandsRequest{Context: req.GetContext()}
	batchGetConcessionStandsReply := new(concessionsv1.BatchGetConcessionStandsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", concessionsConcessionStandServiceName, "BatchGetConcessionStands"), batchGetConcessionStandsRequest, batchGetConcessionStandsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.concessions.v1.ConcessionStandService/BatchGetConcessionStands")
	}
	routes := map[string]func(context.Context, *catalogv1.CalculateBrandRequest, ...grpc.CallOption) (*catalogv1.CalculateBrandResponse, error){
		"primary":  s.brandCalculationClient.CalculateBrand,
		"fallback": s.brandCalculationClientFallback.CalculateBrand,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	calculateBrandRequest := &catalogv1.CalculateBrandRequest{
		Context: req.GetContext(),
		Brand: &catalogv1.Brand{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetProduct().GetAudit().GetUpdatedAt()},
		},
	}
	calculateBrandReply, err := route(ctx, calculateBrandRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandCalculationService/CalculateBrand")
	}
	return &catalogv1.RecallProductResponse{
		Result: &commonv1.OperationResult{
			Message: batchGetConcessionStandsReply.GetResult().GetMessage(),
			Details: calculateBrandReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Product: &catalogv1.Product{ProductId: req.GetProductId()},
		Outcome: &catalogv1.RecallProductResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// GetProductApprovalStatus returns a product.
func (s *ProductApprovalServer) GetProductApprovalStatus(ctx context.Context, req *catalogv1.GetProductApprovalStatusRequest) (*catalogv1.GetProductApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product", req.GetProductId(), "get_product_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get product approval status", "tenant", req.GetContext().GetTenantId())
	watchUsersStream, err := s.userClient.WatchUsers(ctx, &identityv1.WatchUsersRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		Filter: &identityv1.WatchUsersRequest_Filter{
			Labels:       req.GetContext().GetBaggage(),
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/WatchUsers")
	}
	for {
		received, err := watchUsersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/WatchUsers")
		}
		_ = received
	}
	return &catalogv1.GetProductApprovalStatusResponse{
		Product: &catalogv1.Product{ProductId: req.GetProductId()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentProductApproval comments a product.
func (s *ProductApprovalServer) CommentProductApproval(ctx context.Context, req *catalogv1.CommentProductApprovalRequest) (*catalogv1.CommentProductApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "comment product approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	batchCreateProductsStream, err := s.productClient.BatchCreateProducts(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductService/BatchCreateProducts")
	}
	if err := batchCreateProductsStream.Send(&catalogv1.BatchCreateProductsRequest{
		Context: req.GetContext(),
		Chunk: &catalogv1.BatchCreateProductsRequest_Record{Record: &catalogv1.Product{
			Components: map[string]*catalogv1.ProductComponent{"primary": &catalogv1.ProductComponent{
				ComponentId: req.GetProduct().GetComponents()["primary"].GetComponentId(),
			}},
			Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{Locality: req.GetProduct().GetAddress().GetLocality()}},
			Owner:   &identityv1.UserRef{Email: req.GetDetails().GetApprover().GetEmail()},
		}},
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductService/BatchCreateProducts")
	}
	if _, err := batchCreateProductsStream.CloseAndRecv(); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductService/BatchCreateProducts")
	}
	return &catalogv1.CommentProductApprovalResponse{
		Product: &catalogv1.Product{
			ProductId: req.GetProductId(),
			Capacity:  req.GetProduct().GetCapacity(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// dispatchOrderLazy dials acme.dispatch.v1.DispatchOrderService the first time a handler needs it.
func (s *ProductApprovalServer) dispatchOrderLazy() dispatchv1.DispatchOrderServiceClient {
	s.dispatchOrderOnce.Do(func() { s.dispatchOrderCached = dispatchv1.NewDispatchOrderServiceClient(s.conn) })
	return s.dispatchOrderCached
}

// venueQueryLazy dials acme.venues.v1.VenueQueryService the first time a handler needs it.
func (s *ProductApprovalServer) venueQueryLazy() venuesv1.VenueQueryServiceClient {
	s.venueQueryOnce.Do(func() { s.venueQueryCached = venuesv1.NewVenueQueryServiceClient(s.conn) })
	return s.venueQueryCached
}

// productApprovalServerEscalateGiftCardBatchSteps are the ways to run
// acme.giftcards.v1.GiftCardBatchApprovalService/EscalateGiftCardBatch, by name.
var productApprovalServerEscalateGiftCardBatchSteps = map[string]func(*ProductApprovalServer, context.Context, *giftcardsv1.EscalateGiftCardBatchRequest) (*giftcardsv1.EscalateGiftCardBatchResponse, error){
	"default": (*ProductApprovalServer).escalateGiftCardBatchStep,
	"audited": (*ProductApprovalServer).escalateGiftCardBatchAuditedStep,
}

func (s *ProductApprovalServer) escalateGiftCardBatchStep(ctx context.Context, request *giftcardsv1.EscalateGiftCardBatchRequest) (*giftcardsv1.EscalateGiftCardBatchResponse, error) {
	return s.giftCardBatchApprovalClient.EscalateGiftCardBatch(ctx, request)
}

func (s *ProductApprovalServer) escalateGiftCardBatchAuditedStep(ctx context.Context, request *giftcardsv1.EscalateGiftCardBatchRequest) (*giftcardsv1.EscalateGiftCardBatchResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.giftcards.v1.GiftCardBatchApprovalService/EscalateGiftCardBatch")
	return s.giftCardBatchApprovalClient.EscalateGiftCardBatch(ctx, request)
}
