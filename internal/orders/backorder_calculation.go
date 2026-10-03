package orders

import (
	"context"
	"errors"
	"io"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	datasetsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/datasets/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	featurestorev1 "github.com/hounddogai/protomap-test-golang/gen/acme/featurestore/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// BackorderCalculationServer implements `acme.orders.v1.BackorderCalculationService`.
// Computes the figures of a backorder, such as totals, taxes, and estimates.
type BackorderCalculationServer struct {
	ordersv1.UnimplementedBackorderCalculationServiceServer
	store                          *Store
	log                            *slog.Logger
	riskAssessmentValidationClient fraudv1.RiskAssessmentValidationServiceClient
	rewardCalculationClient        loyaltyv1.RewardCalculationServiceClient
	clients                        *registry.Registry
	orderCalculationClient         ordersv1.OrderCalculationServiceClient
	orderCalculationClientFallback ordersv1.OrderCalculationServiceClient
	orderValidationClient          ordersv1.OrderValidationServiceClient
	auditEventValidationClient     auditv1.AuditEventValidationServiceClient
	accessPolicyQueryClient        authorizationv1.AccessPolicyQueryServiceClient
	featureViewQueryClient         featurestorev1.FeatureViewQueryServiceClient
	giftCardCalculationClient      giftcardsv1.GiftCardCalculationServiceClient
	conn                           *grpc.ClientConn
	datasetQueryClient             datasetsv1.DatasetQueryServiceClient
	accessPolicyClient             authorizationv1.AccessPolicyServiceClient
	accessPolicyQueryPort          accessPolicyQueryPort
	orderCalculationPort           orderCalculationPort
	featureFlagValidationClient    featureflagsv1.FeatureFlagValidationServiceClient
}

// NewBackorderCalculationServer wires a BackorderCalculationServer.
func NewBackorderCalculationServer(deps platform.Deps) *BackorderCalculationServer {
	return &BackorderCalculationServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.orders.v1.BackorderCalculationService"),
		riskAssessmentValidationClient: fraudv1.NewRiskAssessmentValidationServiceClient(deps.Conn),
		rewardCalculationClient:        loyaltyv1.NewRewardCalculationServiceClient(deps.Conn),
		clients:                        deps.Clients,
		orderCalculationClient:         ordersv1.NewOrderCalculationServiceClient(deps.Conn),
		orderCalculationClientFallback: ordersv1.NewOrderCalculationServiceClient(deps.Conn),
		orderValidationClient:          ordersv1.NewOrderValidationServiceClient(deps.Conn),
		auditEventValidationClient:     auditv1.NewAuditEventValidationServiceClient(deps.Conn),
		accessPolicyQueryClient:        authorizationv1.NewAccessPolicyQueryServiceClient(deps.Conn),
		featureViewQueryClient:         featurestorev1.NewFeatureViewQueryServiceClient(deps.Conn),
		giftCardCalculationClient:      giftcardsv1.NewGiftCardCalculationServiceClient(deps.Conn),
		conn:                           deps.Conn,
		datasetQueryClient:             datasetsv1.NewDatasetQueryServiceClient(deps.Conn),
		accessPolicyClient:             authorizationv1.NewAccessPolicyServiceClient(deps.Conn),
		accessPolicyQueryPort:          newAccessPolicyQueryPort(deps),
		orderCalculationPort:           newOrderCalculationPort(deps),
		featureFlagValidationClient:    featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn),
	}
}

// CalculateBackorder calculates a backorder.
func (s *BackorderCalculationServer) CalculateBackorder(ctx context.Context, req *ordersv1.CalculateBackorderRequest) (*ordersv1.CalculateBackorderResponse, error) {
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "calculate backorder", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	step := &backorderCalculationServerCalculateBackorderNormalizeRiskAssessmentStep{request: &fraudv1.NormalizeRiskAssessmentRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetBackorder().GetAudit().GetUpdatedAt()},
			Sla:   &fraudv1.RiskAssessmentSla{PausedFor: req.GetBackorder().GetSla().GetPausedFor()},
		},
		Details: &fraudv1.NormalizeRiskAssessmentRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetBackorder().GetRequester().GetName()},
		},
	}}
	if err := s.runCalculateBackorderNormalizeRiskAssessment(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentValidationService/NormalizeRiskAssessment")
	}
	normalizeRiskAssessmentReply := step.reply
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*loyaltyv1.RecalculateRewardsResponse, len(req.GetBackorder().GetComments()))
	for i, comment := range req.GetBackorder().GetComments() {
		group.Go(func() error {
			reply, err := s.rewardCalculationClient.RecalculateRewards(groupCtx, &loyaltyv1.RecalculateRewardsRequest{
				Context: req.GetContext(),
				Reward: &loyaltyv1.Reward{
					Audit: &commonv1.AuditInfo{
						CreatedAt: comment.GetCreatedAt(),
						CreatedBy: req.GetBackorder().GetAudit().GetCreatedBy(),
					},
					Action: &loyaltyv1.Reward_Block{Block: &loyaltyv1.RewardBlockAction{NotifyEmail: comment.GetAuthorEmail()}},
					Owner:  &identityv1.UserRef{Email: req.GetBackorder().GetAssignee().GetEmail()},
				},
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardCalculationService/RecalculateRewards")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	sessionNotification, ok := s.clients.Get("acme.identity.v1.SessionNotificationService").(identityv1.SessionNotificationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.identity.v1.SessionNotificationService")
	}
	unsubscribeSessionUpdatesRequest := &identityv1.UnsubscribeSessionUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.UnsubscribeSessionUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Phone: normalizeRiskAssessmentReply.GetRiskAssessment().GetRequester().GetPhone(),
			},
		},
	}
	unsubscribeSessionUpdatesReply, err := sessionNotification.UnsubscribeSessionUpdates(ctx, unsubscribeSessionUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/UnsubscribeSessionUpdates")
	}
	return &ordersv1.CalculateBackorderResponse{
		Backorder: &ordersv1.Backorder{
			Assignee: &identityv1.UserRef{
				Email: normalizeRiskAssessmentReply.GetRiskAssessment().GetAssignee().GetEmail(),
			},
			Sla: &ordersv1.BackorderSla{
				PausedFor: unsubscribeSessionUpdatesReply.GetSession().GetSla().GetPausedFor(),
			},
			BackorderId: req.GetBackorderId(),
			Requester:   req.GetBackorder().GetRequester(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EstimateBackorder estimates a backorder.
func (s *BackorderCalculationServer) EstimateBackorder(ctx context.Context, req *ordersv1.EstimateBackorderRequest) (*ordersv1.EstimateBackorderResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EstimateBackorder", "error", err)
		return nil, err
	}
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "estimate backorder", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *ordersv1.ExplainOrderCalculationRequest, ...grpc.CallOption) (*ordersv1.ExplainOrderCalculationResponse, error){
		"primary":  s.orderCalculationClient.ExplainOrderCalculation,
		"fallback": s.orderCalculationClientFallback.ExplainOrderCalculation,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	explainOrderCalculationRequest := &ordersv1.ExplainOrderCalculationRequest{
		Context: req.GetContext(),
		Details: &ordersv1.ExplainOrderCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetBackorder().GetAssignee().GetEmail()},
		},
		Order: &ordersv1.Order{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
			},
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetBackorder().GetAudit().GetUpdatedBy()},
		},
	}
	explainOrderCalculationReply, err := route(ctx, explainOrderCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderCalculationService/ExplainOrderCalculation")
	}
	step := &backorderCalculationServerEstimateBackorderPreviewOrderStep{request: &ordersv1.PreviewOrderRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			Counterparty: &customersv1.CustomerRef{Email: req.GetBackorder().GetRequester().GetEmail()},
			BillingAddress: &commonv1.PostalAddress{
				Region: explainOrderCalculationReply.GetOrder().GetBillingAddress().GetRegion(),
			},
		},
	}}
	if err := s.runEstimateBackorderPreviewOrder(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderValidationService/PreviewOrder")
	}
	previewOrderReply := step.reply
	packed, err := anypb.New(req.GetBackorder())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainAuditEventDecisionRequest := &auditv1.ExplainAuditEventDecisionRequest{
		Context: req.GetContext(),
		Details: &auditv1.ExplainAuditEventDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetBackorder().GetRequester().GetPreferredChannel(),
			},
		},
		AuditEvent: &auditv1.AuditEvent{
			Subject: &auditv1.AuditTrailRef{
				DisplayName: explainOrderCalculationReply.GetOrder().GetCounterparty().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	explainAuditEventDecisionReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*auditv1.ExplainAuditEventDecisionResponse, error) {
		return s.auditEventValidationClient.ExplainAuditEventDecision(ctx, explainAuditEventDecisionRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventValidationService/ExplainAuditEventDecision")
	}
	findAccessPoliciesRequest := &authorizationv1.FindAccessPoliciesRequest{
		Context: req.GetContext(),
		Filter: &authorizationv1.FindAccessPoliciesRequest_Filter{
			Region:       explainOrderCalculationReply.GetOrder().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetBackorder().GetAudit().GetCreatedAt(),
			OwnerId:      explainAuditEventDecisionReply.GetAuditEvent().GetSubject().GetAuditTrailId(),
		},
	}
	findAccessPoliciesReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*authorizationv1.FindAccessPoliciesResponse, error) {
		return s.accessPolicyQueryClient.FindAccessPolicies(ctx, findAccessPoliciesRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyQueryService/FindAccessPolicies")
	}
	return &ordersv1.EstimateBackorderResponse{
		Backorder: &ordersv1.Backorder{
			Sla: &ordersv1.BackorderSla{DueAt: explainOrderCalculationReply.GetOrder().GetDueAt()},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: explainOrderCalculationReply.GetOrder().GetAudit().GetUpdatedAt(),
				Etag:      previewOrderReply.GetOrder().GetAudit().GetEtag(),
			},
			BackorderId: req.GetBackorderId(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds:  explainAuditEventDecisionReply.GetResult().GetReferenceIds(),
			Status:        findAccessPoliciesReply.GetResult().GetStatus(),
			CorrelationId: findAccessPoliciesReply.GetResult().GetCorrelationId(),
		},
		Breakdown: &ordersv1.EstimateBackorderResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
	}, nil
}

// QuoteBackorder quotes a backorder.
func (s *BackorderCalculationServer) QuoteBackorder(ctx context.Context, req *ordersv1.QuoteBackorderRequest) (*ordersv1.QuoteBackorderResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected QuoteBackorder", "error", err)
		return nil, err
	}
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "quote backorder", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*featurestorev1.BrowseFeatureViewsResponse, len(req.GetBackorder().GetHistory()))
	for i, item := range req.GetBackorder().GetHistory() {
		group.Go(func() error {
			reply, err := s.featureViewQueryClient.BrowseFeatureViews(groupCtx, &featurestorev1.BrowseFeatureViewsRequest{
				Context: req.GetContext(),
				Filter:  &featurestorev1.BrowseFeatureViewsRequest_Filter{CreatedAfter: item.GetChangedAt()},
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.featurestore.v1.FeatureViewQueryService/BrowseFeatureViews")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	estimateOrderRequest := newEstimateOrderRequest(withEstimateOrderContext(req.GetContext()), withEstimateOrderOrder(&ordersv1.Order{
		BillingAddress: &commonv1.PostalAddress{Locality: req.GetDetails().GetDestination().GetLocality()},
	}), withEstimateOrderDetails(&ordersv1.EstimateOrderRequest_Details{
		Destination: &commonv1.PostalAddress{Recipient: req.GetDetails().GetDestination().GetRecipient()},
	}))
	estimateOrderReply, err := s.orderCalculationClient.EstimateOrder(ctx, estimateOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderCalculationService/EstimateOrder")
	}
	return &ordersv1.QuoteBackorderResponse{
		Backorder: &ordersv1.Backorder{
			Assignee: &identityv1.UserRef{
				Email: estimateOrderReply.GetOrder().GetCounterparty().GetEmail(),
			},
			BackorderId:  req.GetBackorderId(),
			CustomFields: req.GetBackorder().GetCustomFields(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CompareBackorderOptions compares a backorder.
func (s *BackorderCalculationServer) CompareBackorderOptions(ctx context.Context, req *ordersv1.CompareBackorderOptionsRequest) (*ordersv1.CompareBackorderOptionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "compare backorder options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	compareRewardOptionsRequest := &loyaltyv1.CompareRewardOptionsRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.CompareRewardOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{Lines: req.GetDetails().GetDestination().GetLines()},
		},
		Reward: &loyaltyv1.Reward{
			Owner: &identityv1.UserRef{Email: req.GetBackorder().GetAssignee().GetEmail()},
		},
	}
	type compareRewardOptionsResult struct {
		reply *loyaltyv1.CompareRewardOptionsResponse
		err   error
	}
	compareRewardOptionsResults := make(chan compareRewardOptionsResult, 1)
	go func() {
		reply, err := s.rewardCalculationClient.CompareRewardOptions(ctx, compareRewardOptionsRequest)
		compareRewardOptionsResults <- compareRewardOptionsResult{reply, err}
	}()
	var compareRewardOptionsReply *loyaltyv1.CompareRewardOptionsResponse
	select {
	case result := <-compareRewardOptionsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.loyalty.v1.RewardCalculationService/CompareRewardOptions")
		}
		compareRewardOptionsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	calculateGiftCardRequest := buildCalculateGiftCardRequestForCompareBackorderOptions(req)
	calculateGiftCardReply, err := s.giftCardCalculationClient.CalculateGiftCard(ctx, calculateGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/CalculateGiftCard")
	}
	return &ordersv1.CompareBackorderOptionsResponse{
		Backorder: &ordersv1.Backorder{
			Priority: compareRewardOptionsReply.GetReward().GetPriority(),
			Assignee: &identityv1.UserRef{
				Email:       calculateGiftCardReply.GetGiftCard().GetContact().GetEmail(),
				DisplayName: calculateGiftCardReply.GetGiftCard().GetHolder().GetDisplayName(),
			},
			BackorderId: req.GetBackorderId(),
			Status:      req.GetBackorder().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecalculateBackorders recalculates a backorder.
func (s *BackorderCalculationServer) RecalculateBackorders(ctx context.Context, req *ordersv1.RecalculateBackordersRequest) (*ordersv1.RecalculateBackordersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "recalculate backorders", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	estimateOrderRequest := &ordersv1.EstimateOrderRequest{
		Context: req.GetContext(),
		Details: &ordersv1.EstimateOrderRequest_Details{
			Destination: &commonv1.PostalAddress{Locality: req.GetDetails().GetDestination().GetLocality()},
		},
	}
	estimateOrderReply := new(ordersv1.EstimateOrderResponse)
	if err := grpcx.Invoke(ctx, s.conn, ordersv1.OrderCalculationService_EstimateOrder_FullMethodName, estimateOrderRequest, estimateOrderReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderCalculationService/EstimateOrder")
	}
	lintOrdersRequest := &ordersv1.LintOrdersRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			Audit:        &commonv1.AuditInfo{Etag: req.GetBackorder().GetAudit().GetEtag()},
			Counterparty: &customersv1.CustomerRef{Email: req.GetBackorder().GetCustomer().GetEmail()},
		},
	}
	lintOrdersReply, err := grpcx.Call(ctx, s.orderValidationClient.LintOrders, lintOrdersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderValidationService/LintOrders")
	}
	tenantNotification, ok := s.clients.Get("acme.identity.v1.TenantNotificationService").(identityv1.TenantNotificationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.identity.v1.TenantNotificationService")
	}
	packed, err := anypb.New(req.GetBackorder())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	sendTenantTestNoticeRequest := &identityv1.SendTenantTestNoticeRequest{
		Context: req.GetContext(),
		Details: &identityv1.SendTenantTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Name:             req.GetBackorder().GetRequester().GetName(),
				Email:            req.GetBackorder().GetRequester().GetEmail(),
				PreferredChannel: req.GetBackorder().GetRequester().GetPreferredChannel(),
			},
			Payload: packed,
		},
	}
	sendTenantTestNoticeReply, err := tenantNotification.SendTenantTestNotice(ctx, sendTenantTestNoticeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantNotificationService/SendTenantTestNotice")
	}
	for _, item := range req.GetBackorder().GetHistory() {
		if _, err := s.datasetQueryClient.CountDatasets(ctx, &datasetsv1.CountDatasetsRequest{
			Context: req.GetContext(),
			Filter: &datasetsv1.CountDatasetsRequest_Filter{
				CreatedAfter: item.GetChangedAt(),
				Region:       sendTenantTestNoticeReply.GetTenant().GetLocation().GetRegion(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.datasets.v1.DatasetQueryService/CountDatasets")
		}
	}
	return &ordersv1.RecalculateBackordersResponse{
		Backorder: &ordersv1.Backorder{
			Audit: &commonv1.AuditInfo{
				CreatedAt: estimateOrderReply.GetOrder().GetAudit().GetCreatedAt(),
				Etag:      estimateOrderReply.GetOrder().GetAudit().GetEtag(),
			},
			BackorderId:  req.GetBackorderId(),
			CustomFields: req.GetBackorder().GetCustomFields(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: lintOrdersReply.GetResult().GetCorrelationId(),
			Message:       sendTenantTestNoticeReply.GetResult().GetMessage(),
			Status:        "ok",
		},
	}, nil
}

// SimulateBackorder simulates a backorder.
func (s *BackorderCalculationServer) SimulateBackorder(ctx context.Context, req *ordersv1.SimulateBackorderRequest) (*ordersv1.SimulateBackorderResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "simulate backorder", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	featureFlagValidation, ok := s.clients.Get("acme.featureflags.v1.FeatureFlagValidationService").(featureflagsv1.FeatureFlagValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.featureflags.v1.FeatureFlagValidationService")
	}
	previewFeatureFlagRequest := &featureflagsv1.PreviewFeatureFlagRequest{
		Context: req.GetContext(),
		Details: &featureflagsv1.PreviewFeatureFlagRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetBackorder().GetRequester().GetPhone()},
		},
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetBackorder().GetAudit().GetUpdatedAt()},
		},
	}
	previewFeatureFlagReply, err := featureFlagValidation.PreviewFeatureFlag(ctx, previewFeatureFlagRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/PreviewFeatureFlag")
	}
	return &ordersv1.SimulateBackorderResponse{
		Result: &commonv1.OperationResult{
			Message: previewFeatureFlagReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Backorder: &ordersv1.Backorder{
			BackorderId:  req.GetBackorderId(),
			CustomFields: req.GetBackorder().GetCustomFields(),
		},
	}, nil
}

// ExplainBackorderCalculation explains a backorder.
func (s *BackorderCalculationServer) ExplainBackorderCalculation(ctx context.Context, req *ordersv1.ExplainBackorderCalculationRequest) (*ordersv1.ExplainBackorderCalculationResponse, error) {
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "explain backorder calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "email", req.GetBackorder().GetAssignee().GetEmail())
	cloned := proto.Clone(req.GetBackorder()).(*ordersv1.Backorder)
	simulateRewardRequest := &loyaltyv1.SimulateRewardRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Owner: &identityv1.UserRef{
				Email:  tenancy.Value(ctx, "email"),
				UserId: cloned.GetAssignee().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetBackorder().GetAudit().GetCreatedBy()},
		},
	}
	simulateRewardReply, err := grpcx.Call(ctx, s.rewardCalculationClient.SimulateReward, simulateRewardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardCalculationService/SimulateReward")
	}
	watchAccessPoliciesStream, err := s.accessPolicyClient.WatchAccessPolicies(ctx, &authorizationv1.WatchAccessPoliciesRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Name:  req.GetDetails().GetContact().GetName(),
			Email: req.GetBackorder().GetRequester().GetEmail(),
			Phone: req.GetBackorder().GetRequester().GetPhone(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/WatchAccessPolicies")
	}
	for {
		received, err := watchAccessPoliciesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/WatchAccessPolicies")
		}
		_ = received
	}
	browseAccessPoliciesRequest := &authorizationv1.BrowseAccessPoliciesRequest{
		Context: req.GetContext(),
		Filter: &authorizationv1.BrowseAccessPoliciesRequest_Filter{
			CreatedAfter: req.GetBackorder().GetAudit().GetUpdatedAt(),
			Labels:       simulateRewardReply.GetReward().GetParameters(),
		},
	}
	browseAccessPoliciesReply, err := s.accessPolicyQueryPort.BrowseAccessPolicies(ctx, browseAccessPoliciesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyQueryService/BrowseAccessPolicies")
	}
	simulateOrderRequest := &ordersv1.SimulateOrderRequest{}
	simulateOrderRequest.Context = req.GetContext()
	simulateOrderRequest.Order = &ordersv1.Order{
		Lines: []*ordersv1.OrderLine{&ordersv1.OrderLine{
			Tax: &ordersv1.OrderLineTax{
				Amount: simulateRewardReply.GetReward().GetDiscount().GetAmount(),
			},
		}},
	}
	simulateOrderReply, err := s.orderCalculationClient.SimulateOrder(ctx, simulateOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderCalculationService/SimulateOrder")
	}
	return &ordersv1.ExplainBackorderCalculationResponse{
		Backorder: &ordersv1.Backorder{
			Audit: &commonv1.AuditInfo{
				CreatedAt: simulateRewardReply.GetReward().GetAudit().GetCreatedAt(),
			},
			Sla:         &ordersv1.BackorderSla{DueAt: simulateOrderReply.GetOrder().GetDueAt()},
			BackorderId: req.GetBackorderId(),
			Requester:   req.GetBackorder().GetRequester(),
		},
		Result: &commonv1.OperationResult{Status: browseAccessPoliciesReply.GetResult().GetStatus()},
	}, nil
}

// GetBackorderRates returns a backorder.
func (s *BackorderCalculationServer) GetBackorderRates(ctx context.Context, req *ordersv1.GetBackorderRatesRequest) (*ordersv1.GetBackorderRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "backorder", req.GetBackorderId(), "get_backorder_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get backorder rates", "tenant", req.GetContext().GetTenantId())
	calculateOrderRequest := &ordersv1.CalculateOrderRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			Counterparty: &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	calculateOrderReply, err := s.orderCalculationPort.CalculateOrder(ctx, calculateOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderCalculationService/CalculateOrder")
	}
	normalizeFeatureFlagRequest := newNormalizeFeatureFlagRequest(withNormalizeFeatureFlagContext(req.GetContext()), withNormalizeFeatureFlagDetails(&featureflagsv1.NormalizeFeatureFlagRequest_Details{
		Contact: &commonv1.ContactInfo{
			Email: calculateOrderReply.GetOrder().GetCounterparty().GetEmail(),
		},
	}))
	normalizeFeatureFlagReply, err := s.featureFlagValidationClient.NormalizeFeatureFlag(ctx, normalizeFeatureFlagRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/NormalizeFeatureFlag")
	}
	return &ordersv1.GetBackorderRatesResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: calculateOrderReply.GetResult().GetReferenceIds(),
			Status:       calculateOrderReply.GetResult().GetStatus(),
			Message:      normalizeFeatureFlagReply.GetResult().GetMessage(),
		},
		Backorder: &ordersv1.Backorder{BackorderId: req.GetBackorderId()},
	}, nil
}

// backorderCalculationServerCalculateBackorderNormalizeRiskAssessmentStep carries the request of one step of
// CalculateBackorder and, once it ran, its reply.
type backorderCalculationServerCalculateBackorderNormalizeRiskAssessmentStep struct {
	request *fraudv1.NormalizeRiskAssessmentRequest
	reply   *fraudv1.NormalizeRiskAssessmentResponse
}

func (s *BackorderCalculationServer) runCalculateBackorderNormalizeRiskAssessment(ctx context.Context, step *backorderCalculationServerCalculateBackorderNormalizeRiskAssessmentStep) error {
	reply, err := s.riskAssessmentValidationClient.NormalizeRiskAssessment(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// backorderCalculationServerEstimateBackorderPreviewOrderStep carries the request of one step of EstimateBackorder and,
// once it ran, its reply.
type backorderCalculationServerEstimateBackorderPreviewOrderStep struct {
	request *ordersv1.PreviewOrderRequest
	reply   *ordersv1.PreviewOrderResponse
}

func (s *BackorderCalculationServer) runEstimateBackorderPreviewOrder(ctx context.Context, step *backorderCalculationServerEstimateBackorderPreviewOrderStep) error {
	reply, err := s.orderValidationClient.PreviewOrder(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
