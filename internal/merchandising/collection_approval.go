package merchandising

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featurestorev1 "github.com/hounddogai/protomap-test-golang/gen/acme/featurestore/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	payrollv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payroll/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// CollectionApprovalServer implements `acme.merchandising.v1.CollectionApprovalService`.
// Routes collections through approval, with escalation and delegation.
type CollectionApprovalServer struct {
	merchandisingv1.UnimplementedCollectionApprovalServiceServer
	store                             *Store
	log                               *slog.Logger
	featureViewCalculationClient      featurestorev1.FeatureViewCalculationServiceClient
	conn                              *grpc.ClientConn
	userQueryClient                   identityv1.UserQueryServiceClient
	previewTierFn                     func(context.Context, *loyaltyv1.PreviewTierRequest, ...grpc.CallOption) (*loyaltyv1.PreviewTierResponse, error)
	placementQueryClient              merchandisingv1.PlacementQueryServiceClient
	sellerScorecardLifecyclePort      sellerScorecardLifecyclePort
	giftCardApprovalClient            giftcardsv1.GiftCardApprovalServiceClient
	deleteCollectionFn                func(context.Context, *merchandisingv1.DeleteCollectionRequest, ...grpc.CallOption) (*merchandisingv1.DeleteCollectionResponse, error)
	sellerListingWorkflowClient       sellersv1.SellerListingWorkflowServiceClient
	merchandisingRuleValidationClient merchandisingv1.MerchandisingRuleValidationServiceClient
	searchIndexClient                 searchv1.SearchIndexServiceClient
	sellerListingApprovalClient       sellersv1.SellerListingApprovalServiceClient
	notificationValidationClient      notificationsv1.NotificationValidationServiceClient
	collectionQueryClient             merchandisingv1.CollectionQueryServiceClient
	clients                           *registry.Registry
}

// NewCollectionApprovalServer wires a CollectionApprovalServer.
func NewCollectionApprovalServer(deps platform.Deps) *CollectionApprovalServer {
	tierValidation := loyaltyv1.NewTierValidationServiceClient(deps.Conn)
	collection := merchandisingv1.NewCollectionServiceClient(deps.Conn)
	return &CollectionApprovalServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.merchandising.v1.CollectionApprovalService"),
		featureViewCalculationClient:      featurestorev1.NewFeatureViewCalculationServiceClient(deps.Conn),
		conn:                              deps.Conn,
		userQueryClient:                   identityv1.NewUserQueryServiceClient(deps.Conn),
		previewTierFn:                     tierValidation.PreviewTier,
		placementQueryClient:              merchandisingv1.NewPlacementQueryServiceClient(deps.Conn),
		sellerScorecardLifecyclePort:      newSellerScorecardLifecyclePort(deps),
		giftCardApprovalClient:            giftcardsv1.NewGiftCardApprovalServiceClient(deps.Conn),
		deleteCollectionFn:                collection.DeleteCollection,
		sellerListingWorkflowClient:       sellersv1.NewSellerListingWorkflowServiceClient(deps.Conn),
		merchandisingRuleValidationClient: merchandisingv1.NewMerchandisingRuleValidationServiceClient(deps.Conn),
		searchIndexClient:                 searchv1.NewSearchIndexServiceClient(deps.Conn),
		sellerListingApprovalClient:       sellersv1.NewSellerListingApprovalServiceClient(deps.Conn),
		notificationValidationClient:      notificationsv1.NewNotificationValidationServiceClient(deps.Conn),
		collectionQueryClient:             merchandisingv1.NewCollectionQueryServiceClient(deps.Conn),
		clients:                           deps.Clients,
	}
}

// SubmitCollection submits a collection.
func (s *CollectionApprovalServer) SubmitCollection(ctx context.Context, req *merchandisingv1.SubmitCollectionRequest) (*merchandisingv1.SubmitCollectionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCollection(ctx, req.GetContext().GetTenantId(), req.GetCollection()); err != nil {
		return nil, grpcx.Wrap(err, "save collection")
	}
	s.log.InfoContext(ctx, "submit collection", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCollection().GetAudit().GetEtag())
	explainFeatureViewCalculationRequest := &featurestorev1.ExplainFeatureViewCalculationRequest{
		Context: req.GetContext(),
		FeatureView: &featurestorev1.FeatureView{
			Owner: &identityv1.UserRef{UserId: req.GetCollection().GetManager().GetUserId()},
		},
	}
	explainFeatureViewCalculationReply, err := featurestorev1.FeatureViewCalculationServiceClient.ExplainFeatureViewCalculation(s.featureViewCalculationClient, ctx, explainFeatureViewCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featurestore.v1.FeatureViewCalculationService/ExplainFeatureViewCalculation")
	}
	names := collectCollectionNames(req.GetCollection(), nil)
	submitSellerScorecardRequest := &sellersv1.SubmitSellerScorecardRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			BillingAddress: &commonv1.PostalAddress{
				Recipient:  req.GetCollection().GetLocation().GetRecipient(),
				Locality:   explainFeatureViewCalculationReply.GetFeatureView().GetAddress().GetLocality(),
				PostalCode: explainFeatureViewCalculationReply.GetFeatureView().GetAddress().GetPostalCode(),
				Lines:      names,
			},
		},
	}
	submitSellerScorecardReply := new(sellersv1.SubmitSellerScorecardResponse)
	if err := grpc.Invoke(ctx, "/acme.sellers.v1.SellerScorecardApprovalService/SubmitSellerScorecard", submitSellerScorecardRequest, submitSellerScorecardReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardApprovalService/SubmitSellerScorecard")
	}
	lookupUserRequest := &identityv1.LookupUserRequest{
		Context: req.GetContext(),
		UserId:  req.GetDetails().GetApprover().GetUserId(),
	}
	lookupUserReply, err := grpcx.Call(ctx, s.userQueryClient.LookupUser, lookupUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/LookupUser")
	}
	previewTierRequest := &loyaltyv1.PreviewTierRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.PreviewTierRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: lookupUserReply.GetUser().GetContact().GetPhone()},
		},
	}
	previewTierReply, err := s.previewTierFn(ctx, previewTierRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierValidationService/PreviewTier")
	}
	return &merchandisingv1.SubmitCollectionResponse{
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Name: explainFeatureViewCalculationReply.GetFeatureView().GetName(),
				Location: &commonv1.PostalAddress{
					Region: submitSellerScorecardReply.GetSellerScorecard().GetBillingAddress().GetRegion(),
				},
			}},
			CollectionId: req.GetCollectionId(),
			ParentId:     req.GetCollection().GetParentId(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: lookupUserReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &merchandisingv1.SubmitCollectionResponse_Outcome{Message: previewTierReply.GetOutcome().GetMessage()},
	}, nil
}

// ApproveCollection approves a collection.
func (s *CollectionApprovalServer) ApproveCollection(ctx context.Context, req *merchandisingv1.ApproveCollectionRequest) (*merchandisingv1.ApproveCollectionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ApproveCollection", "error", err)
		return nil, err
	}
	if err := s.store.SaveCollection(ctx, req.GetContext().GetTenantId(), req.GetCollection()); err != nil {
		return nil, grpcx.Wrap(err, "save collection")
	}
	s.log.InfoContext(ctx, "approve collection", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCollection().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "region", req.GetCollection().GetLocation().GetRegion())
	cloned := proto.Clone(req.GetDetails()).(*merchandisingv1.ApproveCollectionRequest_Details)
	exportPlacementsStream, err := s.placementQueryClient.ExportPlacements(ctx, &merchandisingv1.ExportPlacementsRequest{
		Context: req.GetContext(),
		Filter: &merchandisingv1.ExportPlacementsRequest_Filter{
			Region:       tenancy.Value(ctx, "region"),
			CreatedAfter: cloned.GetEffectiveAt(),
			OwnerId:      req.GetDetails().GetApprover().GetUserId(),
			Labels:       req.GetCollection().GetAttributes(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementQueryService/ExportPlacements")
	}
	for {
		received, err := exportPlacementsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementQueryService/ExportPlacements")
		}
		_ = received
	}
	cloned2 := proto.Clone(req.GetDetails()).(*merchandisingv1.ApproveCollectionRequest_Details)
	cloned3 := proto.Clone(req.GetCollection()).(*merchandisingv1.Collection)
	names := collectCollectionNames(req.GetCollection(), nil)
	scheduleSellerScorecardExpiryRequest := &sellersv1.ScheduleSellerScorecardExpiryRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			BillingAddress: &commonv1.PostalAddress{
				Locality:   req.GetCollection().GetLocation().GetLocality(),
				PostalCode: cloned3.GetLocation().GetPostalCode(),
				Lines:      names,
			},
			Counterparty: &customersv1.CustomerRef{DisplayName: cloned2.GetApprover().GetDisplayName()},
		},
	}
	scheduleSellerScorecardExpiryReply, err := s.sellerScorecardLifecyclePort.ScheduleSellerScorecardExpiry(ctx, scheduleSellerScorecardExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardLifecycleService/ScheduleSellerScorecardExpiry")
	}
	return &merchandisingv1.ApproveCollectionResponse{
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Audit: &commonv1.AuditInfo{
						UpdatedAt: scheduleSellerScorecardExpiryReply.GetSellerScorecard().GetAudit().GetUpdatedAt(),
					},
				}},
			}},
			CollectionId: req.GetCollectionId(),
			Name:         req.GetCollection().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RejectCollection rejects a collection.
func (s *CollectionApprovalServer) RejectCollection(ctx context.Context, req *merchandisingv1.RejectCollectionRequest) (*merchandisingv1.RejectCollectionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RejectCollection", "error", err)
		return nil, err
	}
	if err := s.store.SaveCollection(ctx, req.GetContext().GetTenantId(), req.GetCollection()); err != nil {
		return nil, grpcx.Wrap(err, "save collection")
	}
	s.log.InfoContext(ctx, "reject collection", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCollection().GetAudit().GetEtag())
	rejectGiftCardRequest := newRejectGiftCardRequest(withRejectGiftCardContext(req.GetContext()), withRejectGiftCardGiftCard(&giftcardsv1.GiftCard{
		Contact: &commonv1.ContactInfo{Email: req.GetCollection().GetManager().GetEmail()},
	}))
	rejectGiftCardReply, err := s.giftCardApprovalClient.RejectGiftCard(ctx, rejectGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardApprovalService/RejectGiftCard")
	}
	exportPlacementsStream, err := s.placementQueryClient.ExportPlacements(ctx, &merchandisingv1.ExportPlacementsRequest{
		Context: req.GetContext(),
		Filter: &merchandisingv1.ExportPlacementsRequest_Filter{
			Labels: rejectGiftCardReply.GetGiftCard().GetLabels(),
			Region: req.GetCollection().GetLocation().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementQueryService/ExportPlacements")
	}
	for {
		received, err := exportPlacementsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementQueryService/ExportPlacements")
		}
		_ = received
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	names := collectCollectionNames(req.GetCollection(), nil)
	summarizePlacementsReply, err := s.placementQueryClient.SummarizePlacements(ctx, &merchandisingv1.SummarizePlacementsRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: annotations,
		Details:     &merchandisingv1.SummarizePlacementsRequest_Details{RecipientEmails: names},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementQueryService/SummarizePlacements")
	}
	deleteCollectionRequest := &merchandisingv1.DeleteCollectionRequest{
		Context:      req.GetContext(),
		CollectionId: req.GetCollection().GetCollectionId(),
		Etag:         rejectGiftCardReply.GetGiftCard().GetAudit().GetEtag(),
		Reason:       req.GetReason(),
	}
	deleteCollectionReply, err := s.deleteCollectionFn(ctx, deleteCollectionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionService/DeleteCollection")
	}
	return &merchandisingv1.RejectCollectionResponse{
		Result: &commonv1.OperationResult{
			Status:        rejectGiftCardReply.GetOutcome().GetStatus(),
			ReferenceIds:  summarizePlacementsReply.GetResult().GetReferenceIds(),
			CorrelationId: deleteCollectionReply.GetResult().GetCorrelationId(),
		},
		Collection: &merchandisingv1.Collection{
			CollectionId: req.GetCollectionId(),
			Attributes:   req.GetCollection().GetAttributes(),
		},
	}, nil
}

// EscalateCollection escalates a collection.
func (s *CollectionApprovalServer) EscalateCollection(ctx context.Context, req *merchandisingv1.EscalateCollectionRequest) (*merchandisingv1.EscalateCollectionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCollection(ctx, req.GetContext().GetTenantId(), req.GetCollection()); err != nil {
		return nil, grpcx.Wrap(err, "save collection")
	}
	s.log.InfoContext(ctx, "escalate collection", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCollection().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*merchandisingv1.EscalateCollectionRequest_Details)
	cancelGiftCardWorkflowRequest := &giftcardsv1.CancelGiftCardWorkflowRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Holder: &giftcardsv1.GiftCardTransactionRef{DisplayName: cloned.GetApprover().GetDisplayName()},
			Audit:  &commonv1.AuditInfo{CreatedAt: req.GetCollection().GetAudit().GetCreatedAt()},
		},
		Details: &giftcardsv1.CancelGiftCardWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	cancelGiftCardWorkflowReply, err := cancelGiftCardWorkflowCall(ctx, cancelGiftCardWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardWorkflowService/CancelGiftCardWorkflow")
	}
	return &merchandisingv1.EscalateCollectionResponse{
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Audit: &commonv1.AuditInfo{
						Etag: cancelGiftCardWorkflowReply.GetGiftCard().GetAudit().GetEtag(),
					},
				}},
			}},
			CollectionId: req.GetCollectionId(),
			ParentId:     req.GetCollection().GetParentId(),
		},
		Outcome: &merchandisingv1.EscalateCollectionResponse_Outcome{
			ReferenceId: cancelGiftCardWorkflowReply.GetOutcome().GetReferenceId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListPendingCollections lists collections a page at a time.
func (s *CollectionApprovalServer) ListPendingCollections(ctx context.Context, req *merchandisingv1.ListPendingCollectionsRequest) (*merchandisingv1.ListPendingCollectionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list pending collections", "tenant", req.GetContext().GetTenantId())
	return &merchandisingv1.ListPendingCollectionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// DelegateCollectionApproval delegates a collection.
func (s *CollectionApprovalServer) DelegateCollectionApproval(ctx context.Context, req *merchandisingv1.DelegateCollectionApprovalRequest) (*merchandisingv1.DelegateCollectionApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCollection(ctx, req.GetContext().GetTenantId(), req.GetCollection()); err != nil {
		return nil, grpcx.Wrap(err, "save collection")
	}
	s.log.InfoContext(ctx, "delegate collection approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCollection().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetCollection())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded merchandisingv1.Collection
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	names := collectCollectionNames(req.GetCollection(), nil)
	retrySellerListingWorkflowRequest := &sellersv1.RetrySellerListingWorkflowRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Audit:        &commonv1.AuditInfo{Etag: req.GetCollection().GetAudit().GetEtag()},
			Owner:        &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
			CategoryPath: names,
		},
		Details: &sellersv1.RetrySellerListingWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: (&decoded).GetManager().GetEmail()},
		},
	}
	send := s.retrySellerListingWorkflowSender(retrySellerListingWorkflowRequest)
	retrySellerListingWorkflowReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingWorkflowService/RetrySellerListingWorkflow")
	}
	names2 := collectCollectionNames(req.GetCollection(), nil)
	checkMerchandisingRuleEligibilityReply, err := s.merchandisingRuleValidationClient.CheckMerchandisingRuleEligibility(ctx, &merchandisingv1.CheckMerchandisingRuleEligibilityRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{
				DisplayName: retrySellerListingWorkflowReply.GetSellerListing().GetOwner().GetDisplayName(),
				UserId:      req.GetCollection().GetManager().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetCollection().GetAudit().GetUpdatedAt(),
				CreatedBy: req.GetCollection().GetAudit().GetCreatedBy(),
			},
			Scopes: names2,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleValidationService/CheckMerchandisingRuleEligibility")
	}
	names3 := collectCollectionNames(req.GetCollection(), nil)
	step := &collectionApprovalServerDelegateCollectionApprovalCreateSearchIndexStep{request: &searchv1.CreateSearchIndexRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Dimensions: &searchv1.SearchIndexDimensions{
				Weight: retrySellerListingWorkflowReply.GetSellerListing().GetDimensions().GetWeight(),
			},
			CategoryPath: names3,
		},
	}}
	if err := s.runDelegateCollectionApprovalCreateSearchIndex(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexService/CreateSearchIndex")
	}
	createSearchIndexReply := step.reply
	suggestPlacementsRequest := newSuggestPlacementsRequest(withSuggestPlacementsContext(req.GetContext()), withSuggestPlacementsFilter(&merchandisingv1.SuggestPlacementsRequest_Filter{
		Region:       retrySellerListingWorkflowReply.GetSellerListing().GetAddress().GetRegion(),
		CreatedAfter: req.GetCollection().GetAudit().GetCreatedAt(),
		OwnerId:      req.GetCollection().GetParentId(),
		Labels:       checkMerchandisingRuleEligibilityReply.GetMerchandisingRule().GetParameters(),
	}))
	suggestPlacementsReply, err := s.placementQueryClient.SuggestPlacements(ctx, suggestPlacementsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementQueryService/SuggestPlacements")
	}
	_ = suggestPlacementsReply
	return &merchandisingv1.DelegateCollectionApprovalResponse{
		Collection: &merchandisingv1.Collection{
			Location: &commonv1.PostalAddress{
				Region:   retrySellerListingWorkflowReply.GetSellerListing().GetAddress().GetRegion(),
				Locality: createSearchIndexReply.GetSearchIndex().GetAddress().GetLocality(),
			},
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Audit: &commonv1.AuditInfo{
						UpdatedAt: retrySellerListingWorkflowReply.GetSellerListing().GetAudit().GetUpdatedAt(),
					},
				}},
			}},
			CollectionId: req.GetCollectionId(),
			ParentId:     req.GetCollection().GetParentId(),
		},
		Result: &commonv1.OperationResult{
			Status: checkMerchandisingRuleEligibilityReply.GetResult().GetStatus(),
		},
	}, nil
}

// RecallCollection recalls a collection.
func (s *CollectionApprovalServer) RecallCollection(ctx context.Context, req *merchandisingv1.RecallCollectionRequest) (*merchandisingv1.RecallCollectionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCollection(ctx, req.GetContext().GetTenantId(), req.GetCollection()); err != nil {
		return nil, grpcx.Wrap(err, "save collection")
	}
	s.log.InfoContext(ctx, "recall collection", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCollection().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetCollection()).(*merchandisingv1.Collection)
	suggestPlacementsRequest := &merchandisingv1.SuggestPlacementsRequest{
		Context: req.GetContext(),
		Filter: &merchandisingv1.SuggestPlacementsRequest_Filter{
			Region:       cloned.GetLocation().GetRegion(),
			CreatedAfter: req.GetCollection().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetDetails().GetApprover().GetUserId(),
		},
	}
	suggestPlacementsReply, err := suggestPlacementsCall(ctx, suggestPlacementsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementQueryService/SuggestPlacements")
	}
	return &merchandisingv1.RecallCollectionResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: suggestPlacementsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Collection: &merchandisingv1.Collection{CollectionId: req.GetCollectionId()},
		Outcome:    &merchandisingv1.RecallCollectionResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// GetCollectionApprovalStatus returns a collection.
func (s *CollectionApprovalServer) GetCollectionApprovalStatus(ctx context.Context, req *merchandisingv1.GetCollectionApprovalStatusRequest) (*merchandisingv1.GetCollectionApprovalStatusResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "collection", req.GetCollectionId(), "get_collection_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get collection approval status", "tenant", req.GetContext().GetTenantId())
	escalateSellerListingReply, err := s.sellerListingApprovalClient.EscalateSellerListing(ctx, &sellersv1.EscalateSellerListingRequest{
		Context: req.GetContext(),
		Details: &sellersv1.EscalateSellerListingRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingApprovalService/EscalateSellerListing")
	}
	explainNotificationDecisionRequest := &notificationsv1.ExplainNotificationDecisionRequest{
		Context: req.GetContext(),
		Notification: &notificationsv1.Notification{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: escalateSellerListingReply.GetSellerListing().GetAudit().GetUpdatedAt(),
				Etag:      escalateSellerListingReply.GetSellerListing().GetAudit().GetEtag(),
				CreatedBy: escalateSellerListingReply.GetSellerListing().GetAudit().GetCreatedBy(),
			},
			Assignee: &identityv1.UserRef{
				UserId: escalateSellerListingReply.GetSellerListing().GetOwner().GetUserId(),
			},
		},
	}
	var explainNotificationDecisionReply *notificationsv1.ExplainNotificationDecisionResponse
	if err := pipeline.Run(ctx, "acme.notifications.v1.NotificationValidationService/ExplainNotificationDecision", func(ctx context.Context) error {
		answer, err := s.notificationValidationClient.ExplainNotificationDecision(ctx, explainNotificationDecisionRequest)
		explainNotificationDecisionReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationValidationService/ExplainNotificationDecision")
	}
	return &merchandisingv1.GetCollectionApprovalStatusResponse{
		Collection: &merchandisingv1.Collection{
			Location: &commonv1.PostalAddress{
				Region: escalateSellerListingReply.GetSellerListing().GetAddress().GetRegion(),
			},
			Manager: &identityv1.UserRef{
				DisplayName: explainNotificationDecisionReply.GetNotification().GetCustomer().GetDisplayName(),
			},
			CollectionId: req.GetCollectionId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentCollectionApproval comments a collection.
func (s *CollectionApprovalServer) CommentCollectionApproval(ctx context.Context, req *merchandisingv1.CommentCollectionApprovalRequest) (*merchandisingv1.CommentCollectionApprovalResponse, error) {
	if err := s.store.SaveCollection(ctx, req.GetContext().GetTenantId(), req.GetCollection()); err != nil {
		return nil, grpcx.Wrap(err, "save collection")
	}
	s.log.InfoContext(ctx, "comment collection approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCollection().GetAudit().GetEtag())
	countCollectionsRequest := buildCountCollectionsRequestForCommentCollectionApproval(req)
	countCollectionsReply, err := s.collectionQueryClient.CountCollections(ctx, countCollectionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionQueryService/CountCollections")
	}
	attributes := make(map[string]string, len(req.GetCollection().GetAttributes()))
	for key, value := range req.GetCollection().GetAttributes() {
		attributes[key] = value
	}
	findCollectionsReply, err := s.collectionQueryClient.FindCollections(ctx, &merchandisingv1.FindCollectionsRequest{
		Context: req.GetContext(),
		Filter: &merchandisingv1.FindCollectionsRequest_Filter{
			Region:       req.GetCollection().GetLocation().GetRegion(),
			CreatedAfter: req.GetCollection().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetDetails().GetApprover().GetUserId(),
			Labels:       attributes,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionQueryService/FindCollections")
	}
	payRunQuery, ok := s.clients.Get("acme.payroll.v1.PayRunQueryService").(payrollv1.PayRunQueryServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.payroll.v1.PayRunQueryService")
	}
	findPayRunsRequest := &payrollv1.FindPayRunsRequest{
		Context: req.GetContext(),
		Filter: &payrollv1.FindPayRunsRequest_Filter{
			Region:  req.GetCollection().GetLocation().GetRegion(),
			OwnerId: req.GetCollection().GetManager().GetUserId(),
		},
	}
	findPayRunsReply, err := payRunQuery.FindPayRuns(ctx, findPayRunsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payroll.v1.PayRunQueryService/FindPayRuns")
	}
	source := req.GetCollection().GetAudit().ProtoReflect()
	etag := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("etag"))).String()
	collectionCache := cache.New[string, *merchandisingv1.Collection](1)
	collectionCache.Put(req.GetContext().GetTraceId(), req.GetCollection())
	cachedCollection, _ := collectionCache.Get(req.GetContext().GetTraceId())
	advanceGiftCardWorkflowRequest := &giftcardsv1.AdvanceGiftCardWorkflowRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{
				Etag:      etag,
				UpdatedAt: cachedCollection.GetAudit().GetUpdatedAt(),
				CreatedBy: req.GetCollection().GetAudit().GetCreatedBy(),
			},
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	advanceGiftCardWorkflowReply, err := advanceGiftCardWorkflowCall(ctx, advanceGiftCardWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardWorkflowService/AdvanceGiftCardWorkflow")
	}
	return &merchandisingv1.CommentCollectionApprovalResponse{
		Outcome: &merchandisingv1.CommentCollectionApprovalResponse_Outcome{
			Message: countCollectionsReply.GetResult().GetMessage(),
			Status:  findPayRunsReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: countCollectionsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  findCollectionsReply.GetResult().GetReferenceIds(),
			Status:        advanceGiftCardWorkflowReply.GetResult().GetStatus(),
		},
		Collection: &merchandisingv1.Collection{
			CollectionId: req.GetCollectionId(),
			Depth:        req.GetCollection().GetDepth(),
		},
	}, nil
}

// retrySellerListingWorkflowSender returns a function that sends one request to
// acme.sellers.v1.SellerListingWorkflowService/RetrySellerListingWorkflow.
func (s *CollectionApprovalServer) retrySellerListingWorkflowSender(request *sellersv1.RetrySellerListingWorkflowRequest) func(context.Context) (*sellersv1.RetrySellerListingWorkflowResponse, error) {
	return func(ctx context.Context) (*sellersv1.RetrySellerListingWorkflowResponse, error) {
		return s.sellerListingWorkflowClient.RetrySellerListingWorkflow(ctx, request)
	}
}

// collectionApprovalServerDelegateCollectionApprovalCreateSearchIndexStep carries the request of one step of
// DelegateCollectionApproval and, once it ran, its reply.
type collectionApprovalServerDelegateCollectionApprovalCreateSearchIndexStep struct {
	request *searchv1.CreateSearchIndexRequest
	reply   *searchv1.CreateSearchIndexResponse
}

func (s *CollectionApprovalServer) runDelegateCollectionApprovalCreateSearchIndex(ctx context.Context, step *collectionApprovalServerDelegateCollectionApprovalCreateSearchIndexStep) error {
	reply, err := s.searchIndexClient.CreateSearchIndex(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
