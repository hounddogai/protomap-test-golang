package catalog

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	dispatchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/dispatch/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
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

// AttributeLifecycleServer implements `acme.catalog.v1.AttributeLifecycleService`.
// Moves an attribute between the states of its lifecycle.
type AttributeLifecycleServer struct {
	catalogv1.UnimplementedAttributeLifecycleServiceServer
	store                           *Store
	log                             *slog.Logger
	brandReportingClient            catalogv1.BrandReportingServiceClient
	brandReportingClientFallback    catalogv1.BrandReportingServiceClient
	disputeReportingClient          paymentsv1.DisputeReportingServiceClient
	dispatchOrderClient             dispatchv1.DispatchOrderServiceClient
	productClient                   catalogv1.ProductServiceClient
	conn                            *grpc.ClientConn
	priceOverrideWorkflowClient     pricingv1.PriceOverrideWorkflowServiceClient
	meteredTierReporting            meteredTierReporting
	productVariantLifecycleClient   catalogv1.ProductVariantLifecycleServiceClient
	productVariantCalculationClient catalogv1.ProductVariantCalculationServiceClient
	payoutApprovalOnce              sync.Once
	payoutApprovalCached            paymentsv1.PayoutApprovalServiceClient
}

// NewAttributeLifecycleServer wires an AttributeLifecycleServer.
func NewAttributeLifecycleServer(deps platform.Deps) *AttributeLifecycleServer {
	return &AttributeLifecycleServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.catalog.v1.AttributeLifecycleService"),
		brandReportingClient:            catalogv1.NewBrandReportingServiceClient(deps.Conn),
		brandReportingClientFallback:    catalogv1.NewBrandReportingServiceClient(deps.Conn),
		disputeReportingClient:          paymentsv1.NewDisputeReportingServiceClient(deps.Conn),
		dispatchOrderClient:             dispatchv1.NewDispatchOrderServiceClient(deps.Conn),
		productClient:                   catalogv1.NewProductServiceClient(deps.Conn),
		conn:                            deps.Conn,
		priceOverrideWorkflowClient:     pricingv1.NewPriceOverrideWorkflowServiceClient(deps.Conn),
		meteredTierReporting:            meteredTierReporting{TierReportingServiceClient: loyaltyv1.NewTierReportingServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		productVariantLifecycleClient:   catalogv1.NewProductVariantLifecycleServiceClient(deps.Conn),
		productVariantCalculationClient: catalogv1.NewProductVariantCalculationServiceClient(deps.Conn),
	}
}

// ActivateAttribute activates an attribute.
func (s *AttributeLifecycleServer) ActivateAttribute(ctx context.Context, req *catalogv1.ActivateAttributeRequest) (*catalogv1.ActivateAttributeResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "attribute", req.GetAttributeId(), "activate_attribute"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate attribute", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *catalogv1.CountBrandTotalsRequest, ...grpc.CallOption) (*catalogv1.CountBrandTotalsResponse, error){
		"primary":  s.brandReportingClient.CountBrandTotals,
		"fallback": s.brandReportingClientFallback.CountBrandTotals,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	cloned := proto.Clone(req.GetDetails()).(*catalogv1.ActivateAttributeRequest_Details)
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	countBrandTotalsRequest := &catalogv1.CountBrandTotalsRequest{
		Context: req.GetContext(),
		Filter: &catalogv1.CountBrandTotalsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      cloned.GetNewOwner().GetUserId(),
			Labels:       annotations,
		},
	}
	countBrandTotalsReply, err := route(ctx, countBrandTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandReportingService/CountBrandTotals")
	}
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNewOwner().GetEmail())
	shareDisputeReportRequest := &paymentsv1.ShareDisputeReportRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.ShareDisputeReportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Phone:            req.GetDetails().GetNotifyContact().GetPhone(),
				Email:            tenancy.Value(ctx, "email"),
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			},
		},
	}
	shareDisputeReportReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*paymentsv1.ShareDisputeReportResponse, error) {
		return s.disputeReportingClient.ShareDisputeReport(ctx, shareDisputeReportRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeReportingService/ShareDisputeReport")
	}
	annotations2 := make(map[string]string, len(req.GetAnnotations()))
	for key2, value2 := range req.GetAnnotations() {
		annotations2[key2] = value2
	}
	scheduleBrandReportRequest := &catalogv1.ScheduleBrandReportRequest{
		Context:     req.GetContext(),
		Annotations: annotations2,
		Reason:      req.GetReason(),
	}
	scheduleBrandReportReply, err := grpcx.Call(ctx, s.brandReportingClient.ScheduleBrandReport, scheduleBrandReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandReportingService/ScheduleBrandReport")
	}
	return &catalogv1.ActivateAttributeResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: countBrandTotalsReply.GetResult().GetCorrelationId(),
			Status:        countBrandTotalsReply.GetResult().GetStatus(),
			ReferenceIds:  scheduleBrandReportReply.GetResult().GetReferenceIds(),
		},
		Outcome: &catalogv1.ActivateAttributeResponse_Outcome{
			Message:     shareDisputeReportReply.GetOutcome().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Attribute: &catalogv1.Attribute{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: shareDisputeReportReply.GetDispute().GetAudit().GetUpdatedAt(),
			},
			AttributeId: req.GetAttributeId(),
		},
	}, nil
}

// SuspendAttribute suspends an attribute.
func (s *AttributeLifecycleServer) SuspendAttribute(ctx context.Context, req *catalogv1.SuspendAttributeRequest) (*catalogv1.SuspendAttributeResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuspendAttribute", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "attribute", req.GetAttributeId(), "suspend_attribute"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend attribute", "tenant", req.GetContext().GetTenantId())
	createDispatchOrderRequest := buildCreateDispatchOrderRequestForSuspendAttribute(req)
	createDispatchOrderReply, err := s.dispatchOrderClient.CreateDispatchOrder(ctx, createDispatchOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.dispatch.v1.DispatchOrderService/CreateDispatchOrder")
	}
	rejectPayoutRequest := &paymentsv1.RejectPayoutRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.RejectPayoutRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: createDispatchOrderReply.GetDispatchOrder().GetCounterparty().GetDisplayName(),
			},
		},
	}
	rejectPayoutReply, err := rejectPayoutCall(ctx, rejectPayoutRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutApprovalService/RejectPayout")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := attributeLifecycleServerCloneProductSteps[mode]
	cloneProductRequest := &catalogv1.CloneProductRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{
				PostalCode: createDispatchOrderReply.GetDispatchOrder().GetBillingAddress().GetPostalCode(),
			}},
		},
	}
	cloneProductReply, err := run(s, ctx, cloneProductRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductService/CloneProduct")
	}
	return &catalogv1.SuspendAttributeResponse{
		Attribute: &catalogv1.Attribute{
			Owner: &identityv1.UserRef{
				DisplayName: createDispatchOrderReply.GetDispatchOrder().GetCounterparty().GetDisplayName(),
			},
			AttributeId: req.GetAttributeId(),
		},
		Result: &commonv1.OperationResult{
			Details: createDispatchOrderReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &catalogv1.SuspendAttributeResponse_Outcome{
			NextSteps:   rejectPayoutReply.GetOutcome().GetNextSteps(),
			Status:      cloneProductReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	}, nil
}

// ReinstateAttribute reinstates an attribute.
func (s *AttributeLifecycleServer) ReinstateAttribute(ctx context.Context, req *catalogv1.ReinstateAttributeRequest) (*catalogv1.ReinstateAttributeResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "attribute", req.GetAttributeId(), "reinstate_attribute"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate attribute", "tenant", req.GetContext().GetTenantId())
	listSearchQuerySubscriptionsRequest := &searchv1.ListSearchQuerySubscriptionsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSearchQuerySubscriptionsRequest_Filter{
			OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
		},
	}
	listSearchQuerySubscriptionsReply := new(searchv1.ListSearchQuerySubscriptionsResponse)
	if err := grpcx.Invoke(ctx, s.conn, searchv1.SearchQueryNotificationService_ListSearchQuerySubscriptions_FullMethodName, listSearchQuerySubscriptionsRequest, listSearchQuerySubscriptionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/ListSearchQuerySubscriptions")
	}
	cloneProductRequest := buildCloneProductRequestForReinstateAttribute(req)
	cloneProductReply, err := s.productClient.CloneProduct(ctx, cloneProductRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductService/CloneProduct")
	}
	return &catalogv1.ReinstateAttributeResponse{
		Result: &commonv1.OperationResult{
			Details: listSearchQuerySubscriptionsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Attribute: &catalogv1.Attribute{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: cloneProductReply.GetProduct().GetAudit().GetUpdatedBy(),
			},
			AttributeId: req.GetAttributeId(),
		},
		Outcome: &catalogv1.ReinstateAttributeResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// TerminateAttribute terminates an attribute.
func (s *AttributeLifecycleServer) TerminateAttribute(ctx context.Context, req *catalogv1.TerminateAttributeRequest) (*catalogv1.TerminateAttributeResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "attribute", req.GetAttributeId(), "terminate_attribute"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate attribute", "tenant", req.GetContext().GetTenantId())
	advancePriceOverrideWorkflowReply, err := s.priceOverrideWorkflowClient.AdvancePriceOverrideWorkflow(ctx, &pricingv1.AdvancePriceOverrideWorkflowRequest{
		Context: req.GetContext(),
		Details: &pricingv1.AdvancePriceOverrideWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Name:             req.GetDetails().GetNotifyContact().GetName(),
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
				Email:            req.GetDetails().GetNewOwner().GetEmail(),
			},
		},
		PriceOverride: &pricingv1.PriceOverride{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetNotifyContact().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideWorkflowService/AdvancePriceOverrideWorkflow")
	}
	return &catalogv1.TerminateAttributeResponse{
		Attribute: &catalogv1.Attribute{
			Audit: &commonv1.AuditInfo{
				Etag: advancePriceOverrideWorkflowReply.GetPriceOverride().GetAudit().GetEtag(),
			},
			AttributeId: req.GetAttributeId(),
		},
		Outcome: &catalogv1.TerminateAttributeResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RenewAttribute renews an attribute.
func (s *AttributeLifecycleServer) RenewAttribute(ctx context.Context, req *catalogv1.RenewAttributeRequest) (*catalogv1.RenewAttributeResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "attribute", req.GetAttributeId(), "renew_attribute"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew attribute", "tenant", req.GetContext().GetTenantId())
	shareTierReportRequest := &loyaltyv1.ShareTierReportRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.ShareTierReportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email: req.GetDetails().GetNewOwner().GetEmail(),
				Name:  req.GetDetails().GetNotifyContact().GetName(),
			},
		},
	}
	shareTierReportReply, err := s.meteredTierReporting.ShareTierReport(ctx, shareTierReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierReportingService/ShareTierReport")
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded catalogv1.RenewAttributeRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	step := &attributeLifecycleServerRenewAttributeScheduleProductVariantExpiryStep{request: &catalogv1.ScheduleProductVariantExpiryRequest{
		Context: req.GetContext(),
		Details: &catalogv1.ScheduleProductVariantExpiryRequest_Details{
			Notify: &commonv1.ContactInfo{
				Phone: (&decoded).GetNotifyContact().GetPhone(),
				Name:  req.GetDetails().GetNotifyContact().GetName(),
			},
		},
		ProductVariant: &catalogv1.ProductVariant{
			Owner: &identityv1.UserRef{UserId: req.GetDetails().GetNewOwner().GetUserId()},
			Components: map[string]*catalogv1.ProductVariantComponent{"primary": &catalogv1.ProductVariantComponent{
				Description: shareTierReportReply.GetTier().GetDescription(),
			}},
		},
	}}
	if err := s.runRenewAttributeScheduleProductVariantExpiry(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantLifecycleService/ScheduleProductVariantExpiry")
	}
	scheduleProductVariantExpiryReply := step.reply
	return &catalogv1.RenewAttributeResponse{
		Attribute: &catalogv1.Attribute{
			Audit: &commonv1.AuditInfo{
				CreatedAt: shareTierReportReply.GetTier().GetAudit().GetCreatedAt(),
				CreatedBy: scheduleProductVariantExpiryReply.GetProductVariant().GetAudit().GetCreatedBy(),
			},
			AttributeId: req.GetAttributeId(),
		},
		Outcome: &catalogv1.RenewAttributeResponse_Outcome{
			Message:     shareTierReportReply.GetOutcome().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExpireAttribute expires an attribute.
func (s *AttributeLifecycleServer) ExpireAttribute(ctx context.Context, req *catalogv1.ExpireAttributeRequest) (*catalogv1.ExpireAttributeResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "attribute", req.GetAttributeId(), "expire_attribute"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire attribute", "tenant", req.GetContext().GetTenantId())
	detailsCache := cache.New[string, *catalogv1.ExpireAttributeRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	simulateProductVariantRequest := &catalogv1.SimulateProductVariantRequest{
		Context: req.GetContext(),
		ProductVariant: &catalogv1.ProductVariant{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
				UserId:      req.GetDetails().GetNewOwner().GetUserId(),
				Email:       cachedDetails.GetNotifyContact().GetEmail(),
			},
		},
	}
	simulateProductVariantReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*catalogv1.SimulateProductVariantResponse, error) {
		return s.productVariantCalculationClient.SimulateProductVariant(ctx, simulateProductVariantRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantCalculationService/SimulateProductVariant")
	}
	return &catalogv1.ExpireAttributeResponse{
		Attribute: &catalogv1.Attribute{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: simulateProductVariantReply.GetProductVariant().GetAudit().GetUpdatedBy(),
			},
			AttributeId: req.GetAttributeId(),
		},
		Outcome: &catalogv1.ExpireAttributeResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetAttributeLifecycle returns an attribute.
func (s *AttributeLifecycleServer) GetAttributeLifecycle(ctx context.Context, req *catalogv1.GetAttributeLifecycleRequest) (*catalogv1.GetAttributeLifecycleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "attribute", req.GetAttributeId(), "get_attribute_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get attribute lifecycle", "tenant", req.GetContext().GetTenantId())
	listPendingPayoutsRequest := &paymentsv1.ListPendingPayoutsRequest{
		Context: req.GetContext(),
		Filter: &paymentsv1.ListPendingPayoutsRequest_Filter{
			OwnerId:      req.GetContext().GetActor().GetUserId(),
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	}
	listPendingPayoutsReply, err := s.payoutApprovalLazy().ListPendingPayouts(ctx, listPendingPayoutsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutApprovalService/ListPendingPayouts")
	}
	return &catalogv1.GetAttributeLifecycleResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: listPendingPayoutsReply.GetResult().GetReferenceIds(),
			Message:      listPendingPayoutsReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		Attribute: &catalogv1.Attribute{AttributeId: req.GetAttributeId()},
	}, nil
}

// ScheduleAttributeExpiry schedules an attribute.
func (s *AttributeLifecycleServer) ScheduleAttributeExpiry(ctx context.Context, req *catalogv1.ScheduleAttributeExpiryRequest) (*catalogv1.ScheduleAttributeExpiryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScheduleAttributeExpiry", "error", err)
		return nil, err
	}
	if err := s.store.SaveAttribute(ctx, req.GetContext().GetTenantId(), req.GetAttribute()); err != nil {
		return nil, grpcx.Wrap(err, "save attribute")
	}
	s.log.InfoContext(ctx, "schedule attribute expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetAttribute().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	scheduleBrandReportRequest := &catalogv1.ScheduleBrandReportRequest{
		Context:     req.GetContext(),
		Reason:      req.GetAttribute().GetBlock().GetReason(),
		Annotations: annotations,
	}
	scheduleBrandReportReply := new(catalogv1.ScheduleBrandReportResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", catalogBrandReportingServiceName, "ScheduleBrandReport"), scheduleBrandReportRequest, scheduleBrandReportReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandReportingService/ScheduleBrandReport")
	}
	pauseRewardWorkflowRequest := &loyaltyv1.PauseRewardWorkflowRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Action: &loyaltyv1.Reward_Route{Route: &loyaltyv1.RewardRouteAction{Weight: req.GetAttribute().GetRoute().GetWeight()}},
			Owner:  &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
		},
		Details: &loyaltyv1.PauseRewardWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: req.GetAttribute().GetOwner().GetEmail(),
				Name:  req.GetDetails().GetNotify().GetName(),
			},
		},
	}
	pauseRewardWorkflowReply := new(loyaltyv1.PauseRewardWorkflowResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", loyaltyRewardWorkflowServiceName, "PauseRewardWorkflow"), pauseRewardWorkflowRequest, pauseRewardWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardWorkflowService/PauseRewardWorkflow")
	}
	return &catalogv1.ScheduleAttributeExpiryResponse{
		Outcome: &catalogv1.ScheduleAttributeExpiryResponse_Outcome{Message: scheduleBrandReportReply.GetResult().GetMessage()},
		Attribute: &catalogv1.Attribute{
			Priority:    pauseRewardWorkflowReply.GetReward().GetPriority(),
			AttributeId: req.GetAttributeId(),
			Parameters:  req.GetDetails().GetParameters(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// attributeLifecycleServerCloneProductSteps are the ways to run acme.catalog.v1.ProductService/CloneProduct, by name.
var attributeLifecycleServerCloneProductSteps = map[string]func(*AttributeLifecycleServer, context.Context, *catalogv1.CloneProductRequest) (*catalogv1.CloneProductResponse, error){
	"default": (*AttributeLifecycleServer).cloneProductStep,
	"audited": (*AttributeLifecycleServer).cloneProductAuditedStep,
}

func (s *AttributeLifecycleServer) cloneProductStep(ctx context.Context, request *catalogv1.CloneProductRequest) (*catalogv1.CloneProductResponse, error) {
	return s.productClient.CloneProduct(ctx, request)
}

func (s *AttributeLifecycleServer) cloneProductAuditedStep(ctx context.Context, request *catalogv1.CloneProductRequest) (*catalogv1.CloneProductResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.catalog.v1.ProductService/CloneProduct")
	return s.productClient.CloneProduct(ctx, request)
}

// attributeLifecycleServerRenewAttributeScheduleProductVariantExpiryStep carries the request of one step of
// RenewAttribute and, once it ran, its reply.
type attributeLifecycleServerRenewAttributeScheduleProductVariantExpiryStep struct {
	request *catalogv1.ScheduleProductVariantExpiryRequest
	reply   *catalogv1.ScheduleProductVariantExpiryResponse
}

func (s *AttributeLifecycleServer) runRenewAttributeScheduleProductVariantExpiry(ctx context.Context, step *attributeLifecycleServerRenewAttributeScheduleProductVariantExpiryStep) error {
	reply, err := s.productVariantLifecycleClient.ScheduleProductVariantExpiry(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// payoutApprovalLazy dials acme.payments.v1.PayoutApprovalService the first time a handler needs it.
func (s *AttributeLifecycleServer) payoutApprovalLazy() paymentsv1.PayoutApprovalServiceClient {
	s.payoutApprovalOnce.Do(func() { s.payoutApprovalCached = paymentsv1.NewPayoutApprovalServiceClient(s.conn) })
	return s.payoutApprovalCached
}
