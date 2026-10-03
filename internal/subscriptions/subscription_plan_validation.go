package subscriptions

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
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
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// SubscriptionPlanValidationServer implements `acme.subscriptions.v1.SubscriptionPlanValidationService`.
// Checks subscription plans against the rules before anything acts on them.
type SubscriptionPlanValidationServer struct {
	subscriptionsv1.UnimplementedSubscriptionPlanValidationServiceServer
	store                          *Store
	log                            *slog.Logger
	tenantAdminClient              identityv1.TenantAdminServiceClient
	collectionQueryClient          merchandisingv1.CollectionQueryServiceClient
	clients                        *registry.Registry
	notificationValidationClient   notificationsv1.NotificationValidationServiceClient
	bus                            *eventbus.Bus
	meteredProductReview           meteredProductReview
	featureFlagClient              featureflagsv1.FeatureFlagServiceClient
	conn                           *grpc.ClientConn
	reviewModerationQueryClient    reviewsv1.ReviewModerationQueryServiceClient
	tenantNotificationClient       identityv1.TenantNotificationServiceClient
	tenantAdminPort                tenantAdminPort
	giftCardBatchCalculationClient giftcardsv1.GiftCardBatchCalculationServiceClient
}

// NewSubscriptionPlanValidationServer wires a SubscriptionPlanValidationServer.
func NewSubscriptionPlanValidationServer(deps platform.Deps) *SubscriptionPlanValidationServer {
	return &SubscriptionPlanValidationServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.subscriptions.v1.SubscriptionPlanValidationService"),
		tenantAdminClient:              identityv1.NewTenantAdminServiceClient(deps.Conn),
		collectionQueryClient:          merchandisingv1.NewCollectionQueryServiceClient(deps.Conn),
		clients:                        deps.Clients,
		notificationValidationClient:   notificationsv1.NewNotificationValidationServiceClient(deps.Conn),
		bus:                            deps.Bus,
		meteredProductReview:           meteredProductReview{ProductReviewServiceClient: reviewsv1.NewProductReviewServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		featureFlagClient:              featureflagsv1.NewFeatureFlagServiceClient(deps.Conn),
		conn:                           deps.Conn,
		reviewModerationQueryClient:    reviewsv1.NewReviewModerationQueryServiceClient(deps.Conn),
		tenantNotificationClient:       identityv1.NewTenantNotificationServiceClient(deps.Conn),
		tenantAdminPort:                newTenantAdminPort(deps),
		giftCardBatchCalculationClient: giftcardsv1.NewGiftCardBatchCalculationServiceClient(deps.Conn),
	}
}

// ValidateSubscriptionPlan validates a subscription plan.
func (s *SubscriptionPlanValidationServer) ValidateSubscriptionPlan(ctx context.Context, req *subscriptionsv1.ValidateSubscriptionPlanRequest) (*subscriptionsv1.ValidateSubscriptionPlanResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSubscriptionPlan(ctx, req.GetContext().GetTenantId(), req.GetSubscriptionPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription plan")
	}
	s.log.InfoContext(ctx, "validate subscription plan", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscriptionPlan().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "tenant_id", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*subscriptionsv1.ValidateSubscriptionPlanRequest_Details)
	restoreTenantRequest := &identityv1.RestoreTenantRequest{
		Context:  req.GetContext(),
		TenantId: tenancy.Value(ctx, "tenant_id"),
		Details: &identityv1.RestoreTenantRequest_Details{
			NewOwner: &identityv1.UserRef{
				UserId: req.GetSubscriptionPlan().GetOwner().GetUserId(),
				Email:  cloned.GetContact().GetEmail(),
			},
		},
	}
	type restoreTenantResult struct {
		reply *identityv1.RestoreTenantResponse
		err   error
	}
	restoreTenantResults := make(chan restoreTenantResult, 1)
	go func() {
		reply, err := s.tenantAdminClient.RestoreTenant(ctx, restoreTenantRequest)
		restoreTenantResults <- restoreTenantResult{reply, err}
	}()
	var restoreTenantReply *identityv1.RestoreTenantResponse
	select {
	case result := <-restoreTenantResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.identity.v1.TenantAdminService/RestoreTenant")
		}
		restoreTenantReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &subscriptionsv1.ValidateSubscriptionPlanResponse{
		Outcome: &subscriptionsv1.ValidateSubscriptionPlanResponse_Outcome{Message: restoreTenantReply.GetOutcome().GetMessage()},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			SubscriptionPlanId: req.GetSubscriptionPlanId(),
			Description:        req.GetSubscriptionPlan().GetDescription(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PreviewSubscriptionPlan previews a subscription plan.
func (s *SubscriptionPlanValidationServer) PreviewSubscriptionPlan(ctx context.Context, req *subscriptionsv1.PreviewSubscriptionPlanRequest) (*subscriptionsv1.PreviewSubscriptionPlanResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSubscriptionPlan(ctx, req.GetContext().GetTenantId(), req.GetSubscriptionPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription plan")
	}
	s.log.InfoContext(ctx, "preview subscription plan", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscriptionPlan().GetAudit().GetEtag())
	summarizeCollectionsReply, err := s.collectionQueryClient.SummarizeCollections(ctx, &merchandisingv1.SummarizeCollectionsRequest{
		Context: req.GetContext(),
		Reason:  req.GetSubscriptionPlan().GetBlock().GetReason(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionQueryService/SummarizeCollections")
	}
	loyaltyMemberQuery, ok := s.clients.Get("acme.loyalty.v1.LoyaltyMemberQueryService").(loyaltyv1.LoyaltyMemberQueryServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.loyalty.v1.LoyaltyMemberQueryService")
	}
	encoded, err := protojson.Marshal(req.GetSubscriptionPlan())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded subscriptionsv1.SubscriptionPlan
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	ctx = tenancy.With(ctx, "subscription_plan_id", req.GetSubscriptionPlanId())
	findLoyaltyMembersRequest := &loyaltyv1.FindLoyaltyMembersRequest{
		Context: req.GetContext(),
		Filter: &loyaltyv1.FindLoyaltyMembersRequest_Filter{
			CreatedAfter: (&decoded).GetAudit().GetCreatedAt(),
			OwnerId:      tenancy.Value(ctx, "subscription_plan_id"),
			Labels:       summarizeCollectionsReply.GetResult().GetDetails(),
		},
	}
	findLoyaltyMembersReply, err := loyaltyMemberQuery.FindLoyaltyMembers(ctx, findLoyaltyMembersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberQueryService/FindLoyaltyMembers")
	}
	return &subscriptionsv1.PreviewSubscriptionPlanResponse{
		Result: &commonv1.OperationResult{
			Message: summarizeCollectionsReply.GetResult().GetMessage(),
			Status:  summarizeCollectionsReply.GetResult().GetStatus(),
		},
		Outcome: &subscriptionsv1.PreviewSubscriptionPlanResponse_Outcome{Status: findLoyaltyMembersReply.GetResult().GetStatus()},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			SubscriptionPlanId: req.GetSubscriptionPlanId(),
			Status:             req.GetSubscriptionPlan().GetStatus(),
		},
	}, nil
}

// CheckSubscriptionPlanEligibility checks a subscription plan.
func (s *SubscriptionPlanValidationServer) CheckSubscriptionPlanEligibility(ctx context.Context, req *subscriptionsv1.CheckSubscriptionPlanEligibilityRequest) (*subscriptionsv1.CheckSubscriptionPlanEligibilityResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSubscriptionPlan(ctx, req.GetContext().GetTenantId(), req.GetSubscriptionPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription plan")
	}
	s.log.InfoContext(ctx, "check subscription plan eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscriptionPlan().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetSubscriptionPlan()).(*subscriptionsv1.SubscriptionPlan)
	explainNotificationDecisionReply, err := s.notificationValidationClient.ExplainNotificationDecision(ctx, &notificationsv1.ExplainNotificationDecisionRequest{
		Context: req.GetContext(),
		Details: &notificationsv1.ExplainNotificationDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
		},
		Notification: &notificationsv1.Notification{
			Origin: &notificationsv1.Notification_Customer{Customer: &notificationsv1.NotificationTemplateRef{DisplayName: cloned.GetOwner().GetDisplayName()}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationValidationService/ExplainNotificationDecision")
	}
	return &subscriptionsv1.CheckSubscriptionPlanEligibilityResponse{
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Owner: &identityv1.UserRef{
				DisplayName: explainNotificationDecisionReply.GetNotification().GetAssignee().GetDisplayName(),
			},
			SubscriptionPlanId: req.GetSubscriptionPlanId(),
			Effective:          req.GetSubscriptionPlan().GetEffective(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizeSubscriptionPlan normalizes a subscription plan.
func (s *SubscriptionPlanValidationServer) NormalizeSubscriptionPlan(ctx context.Context, req *subscriptionsv1.NormalizeSubscriptionPlanRequest) (*subscriptionsv1.NormalizeSubscriptionPlanResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected NormalizeSubscriptionPlan", "error", err)
		return nil, err
	}
	if err := s.store.SaveSubscriptionPlan(ctx, req.GetContext().GetTenantId(), req.GetSubscriptionPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription plan")
	}
	s.log.InfoContext(ctx, "normalize subscription plan", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscriptionPlan().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "subscriptions.normalize_subscription_plan.search_product_reviews",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"filter.owner_id":   req.GetSubscriptionPlan().GetSubscriptionPlanId(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	ctx = tenancy.With(ctx, "user_id", req.GetSubscriptionPlan().GetOwner().GetUserId())
	unlockTenantRequest := &identityv1.UnlockTenantRequest{}
	unlockTenantRequest.Context = req.GetContext()
	unlockTenantRequest.TenantId = req.GetContext().GetTenantId()
	unlockTenantRequest.Details = &identityv1.UnlockTenantRequest_Details{
		NewOwner: &identityv1.UserRef{
			UserId: tenancy.Value(ctx, "user_id"),
			Email:  req.GetSubscriptionPlan().GetOwner().GetEmail(),
		},
	}
	unlockTenantReply, err := s.tenantAdminClient.UnlockTenant(ctx, unlockTenantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/UnlockTenant")
	}
	batchGetProductReviewsRequest := &reviewsv1.BatchGetProductReviewsRequest{Context: req.GetContext()}
	batchGetProductReviewsReply, err := s.meteredProductReview.BatchGetProductReviews(ctx, batchGetProductReviewsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewService/BatchGetProductReviews")
	}
	return &subscriptionsv1.NormalizeSubscriptionPlanResponse{
		Result: &commonv1.OperationResult{
			Message: unlockTenantReply.GetOutcome().GetMessage(),
			Details: batchGetProductReviewsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			SubscriptionPlanId: req.GetSubscriptionPlanId(),
			Status:             req.GetSubscriptionPlan().GetStatus(),
		},
	}, nil
}

// ScoreSubscriptionPlan scores a subscription plan.
func (s *SubscriptionPlanValidationServer) ScoreSubscriptionPlan(ctx context.Context, req *subscriptionsv1.ScoreSubscriptionPlanRequest) (*subscriptionsv1.ScoreSubscriptionPlanResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSubscriptionPlan(ctx, req.GetContext().GetTenantId(), req.GetSubscriptionPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription plan")
	}
	s.log.InfoContext(ctx, "score subscription plan", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscriptionPlan().GetAudit().GetEtag())
	batchGetFeatureFlagsRequest := &featureflagsv1.BatchGetFeatureFlagsRequest{Context: req.GetContext()}
	type batchGetFeatureFlagsResult struct {
		reply *featureflagsv1.BatchGetFeatureFlagsResponse
		err   error
	}
	batchGetFeatureFlagsResults := make(chan batchGetFeatureFlagsResult, 1)
	go func() {
		reply, err := s.featureFlagClient.BatchGetFeatureFlags(ctx, batchGetFeatureFlagsRequest)
		batchGetFeatureFlagsResults <- batchGetFeatureFlagsResult{reply, err}
	}()
	var batchGetFeatureFlagsReply *featureflagsv1.BatchGetFeatureFlagsResponse
	select {
	case result := <-batchGetFeatureFlagsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.featureflags.v1.FeatureFlagService/BatchGetFeatureFlags")
		}
		batchGetFeatureFlagsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	scoreSellerListingRequest := &sellersv1.ScoreSellerListingRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Locator: &sellersv1.SellerListing_Address{Address: &commonv1.PostalAddress{
				Recipient: req.GetDetails().GetDestination().GetRecipient(),
				Lines:     req.GetDetails().GetDestination().GetLines(),
				Region:    req.GetDetails().GetDestination().GetRegion(),
			}},
			Owner: &identityv1.UserRef{Email: req.GetSubscriptionPlan().GetOwner().GetEmail()},
		},
	}
	scoreSellerListingReply := new(sellersv1.ScoreSellerListingResponse)
	if err := grpc.Invoke(ctx, "/acme.sellers.v1.SellerListingValidationService/ScoreSellerListing", scoreSellerListingRequest, scoreSellerListingReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingValidationService/ScoreSellerListing")
	}
	var reason string
	switch choice := req.GetSubscriptionPlan().GetAction().(type) {
	case *subscriptionsv1.SubscriptionPlan_Route:
		reason = choice.Route.GetTarget()
	case *subscriptionsv1.SubscriptionPlan_Block:
		reason = choice.Block.GetReason()
	}
	summarizeReviewModerationsRequest := &reviewsv1.SummarizeReviewModerationsRequest{
		Context:     req.GetContext(),
		Reason:      reason,
		Annotations: req.GetAnnotations(),
		Details: &reviewsv1.SummarizeReviewModerationsRequest_Details{
			Period:  req.GetSubscriptionPlan().GetEffective(),
			Filters: scoreSellerListingReply.GetResult().GetDetails(),
		},
	}
	var summarizeReviewModerationsReply *reviewsv1.SummarizeReviewModerationsResponse
	if err := pipeline.Run(ctx, "acme.reviews.v1.ReviewModerationQueryService/SummarizeReviewModerations", func(ctx context.Context) error {
		answer, err := s.reviewModerationQueryClient.SummarizeReviewModerations(ctx, summarizeReviewModerationsRequest)
		summarizeReviewModerationsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationQueryService/SummarizeReviewModerations")
	}
	var notifyEmail string
	switch choice2 := req.GetSubscriptionPlan().GetAction().(type) {
	case *subscriptionsv1.SubscriptionPlan_Route:
		notifyEmail = choice2.Route.GetTarget()
	case *subscriptionsv1.SubscriptionPlan_Block:
		notifyEmail = choice2.Block.GetNotifyEmail()
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	packed, err := anypb.New(req.GetSubscriptionPlan())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	subscribeTenantUpdatesRequest := &identityv1.SubscribeTenantUpdatesRequest{}
	subscribeTenantUpdatesRequest.Context = req.GetContext()
	subscribeTenantUpdatesRequest.Details = &identityv1.SubscribeTenantUpdatesRequest_Details{
		Recipient: &commonv1.ContactInfo{Email: notifyEmail},
		Payload:   packed,
	}
	subscribeTenantUpdatesRequest.Reason = req.GetSubscriptionPlan().GetBlock().GetReason()
	subscribeTenantUpdatesRequest.Annotations = annotations
	subscribeTenantUpdatesRequest.TenantId = req.GetContext().GetTenantId()
	subscribeTenantUpdatesReply, err := s.tenantNotificationClient.SubscribeTenantUpdates(ctx, subscribeTenantUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantNotificationService/SubscribeTenantUpdates")
	}
	return &subscriptionsv1.ScoreSubscriptionPlanResponse{
		Result: &commonv1.OperationResult{
			Details: batchGetFeatureFlagsReply.GetResult().GetDetails(),
			Status:  summarizeReviewModerationsReply.GetResult().GetStatus(),
		},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: scoreSellerListingReply.GetSellerListing().GetAudit().GetUpdatedAt(),
				UpdatedBy: scoreSellerListingReply.GetSellerListing().GetAudit().GetUpdatedBy(),
			},
			Name:               subscribeTenantUpdatesReply.GetTenant().GetName(),
			SubscriptionPlanId: req.GetSubscriptionPlanId(),
			Status:             req.GetSubscriptionPlan().GetStatus(),
		},
	}, nil
}

// LintSubscriptionPlans lints a subscription plan.
func (s *SubscriptionPlanValidationServer) LintSubscriptionPlans(ctx context.Context, req *subscriptionsv1.LintSubscriptionPlansRequest) (*subscriptionsv1.LintSubscriptionPlansResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSubscriptionPlan(ctx, req.GetContext().GetTenantId(), req.GetSubscriptionPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription plan")
	}
	s.log.InfoContext(ctx, "lint subscription plans", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscriptionPlan().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "email", req.GetSubscriptionPlan().GetOwner().GetEmail())
	restoreTenantReply, err := s.tenantAdminClient.RestoreTenant(ctx, &identityv1.RestoreTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Details: &identityv1.RestoreTenantRequest_Details{
			NotifyContact: &commonv1.ContactInfo{
				Name:  req.GetDetails().GetContact().GetName(),
				Email: tenancy.Value(ctx, "email"),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/RestoreTenant")
	}
	return &subscriptionsv1.LintSubscriptionPlansResponse{
		Result: &commonv1.OperationResult{Status: restoreTenantReply.GetResult().GetStatus()},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			SubscriptionPlanId: req.GetSubscriptionPlanId(),
			Name:               req.GetSubscriptionPlan().GetName(),
		},
	}, nil
}

// GetSubscriptionPlanRules returns a subscription plan.
func (s *SubscriptionPlanValidationServer) GetSubscriptionPlanRules(ctx context.Context, req *subscriptionsv1.GetSubscriptionPlanRulesRequest) (*subscriptionsv1.GetSubscriptionPlanRulesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "subscription_plan", req.GetSubscriptionPlanId(), "get_subscription_plan_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get subscription plan rules", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	cloned2 := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	cloned3 := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	archiveTenantRequest := &identityv1.ArchiveTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Details: &identityv1.ArchiveTenantRequest_Details{
			NewOwner: &identityv1.UserRef{
				Email:  cloned.GetActor().GetEmail(),
				UserId: cloned2.GetActor().GetUserId(),
			},
			EffectiveAt: cloned3.GetDeadline(),
		},
		Annotations: req.GetContext().GetBaggage(),
	}
	archiveTenantReply, err := s.tenantAdminPort.ArchiveTenant(ctx, archiveTenantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/ArchiveTenant")
	}
	explainGiftCardBatchCalculationRequest := &giftcardsv1.ExplainGiftCardBatchCalculationRequest{}
	explainGiftCardBatchCalculationRequest.Context = req.GetContext()
	explainGiftCardBatchCalculationRequest.GiftCardBatch = &giftcardsv1.GiftCardBatch{
		Counterparty: &giftcardsv1.GiftCardRef{
			DisplayName: archiveTenantReply.GetTenant().GetManager().GetDisplayName(),
		},
		BillingAddress: &commonv1.PostalAddress{
			Recipient: archiveTenantReply.GetTenant().GetLocation().GetRecipient(),
		},
	}
	explainGiftCardBatchCalculationReply, err := s.giftCardBatchCalculationClient.ExplainGiftCardBatchCalculation(ctx, explainGiftCardBatchCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchCalculationService/ExplainGiftCardBatchCalculation")
	}
	return &subscriptionsv1.GetSubscriptionPlanRulesResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: archiveTenantReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Audit: &commonv1.AuditInfo{
				Etag:      explainGiftCardBatchCalculationReply.GetGiftCardBatch().GetAudit().GetEtag(),
				CreatedAt: explainGiftCardBatchCalculationReply.GetGiftCardBatch().GetAudit().GetCreatedAt(),
			},
			SubscriptionPlanId: req.GetSubscriptionPlanId(),
		},
	}, nil
}
