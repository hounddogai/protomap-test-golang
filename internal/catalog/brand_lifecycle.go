package catalog

import (
	"context"
	"errors"
	"io"
	"log/slog"

	assetsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/assets/v1"
	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
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

// BrandLifecycleServer implements `acme.catalog.v1.BrandLifecycleService`.
// Moves a brand between the states of its lifecycle.
type BrandLifecycleServer struct {
	catalogv1.UnimplementedBrandLifecycleServiceServer
	store                           *Store
	log                             *slog.Logger
	productVariantReportingClient   catalogv1.ProductVariantReportingServiceClient
	conn                            *grpc.ClientConn
	brandReportingClient            catalogv1.BrandReportingServiceClient
	payoutSyncClient                paymentsv1.PayoutSyncServiceClient
	searchQueryHistoryClient        searchv1.SearchQueryHistoryServiceClient
	clients                         *registry.Registry
	searchIndexQueryPort            searchIndexQueryPort
	meteredCategoryCalculation      meteredCategoryCalculation
	attributeReportingClient        catalogv1.AttributeReportingServiceClient
	apiCredentialNotificationClient identityv1.ApiCredentialNotificationServiceClient
	searchIndexQueryClient          searchv1.SearchIndexQueryServiceClient
	auditEventQueryPort             auditEventQueryPort
	productVariantValidationClient  catalogv1.ProductVariantValidationServiceClient
}

// NewBrandLifecycleServer wires a BrandLifecycleServer.
func NewBrandLifecycleServer(deps platform.Deps) *BrandLifecycleServer {
	return &BrandLifecycleServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.catalog.v1.BrandLifecycleService"),
		productVariantReportingClient:   catalogv1.NewProductVariantReportingServiceClient(deps.Conn),
		conn:                            deps.Conn,
		brandReportingClient:            catalogv1.NewBrandReportingServiceClient(deps.Conn),
		payoutSyncClient:                paymentsv1.NewPayoutSyncServiceClient(deps.Conn),
		searchQueryHistoryClient:        searchv1.NewSearchQueryHistoryServiceClient(deps.Conn),
		clients:                         deps.Clients,
		searchIndexQueryPort:            newSearchIndexQueryPort(deps),
		meteredCategoryCalculation:      meteredCategoryCalculation{CategoryCalculationServiceClient: catalogv1.NewCategoryCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		attributeReportingClient:        catalogv1.NewAttributeReportingServiceClient(deps.Conn),
		apiCredentialNotificationClient: identityv1.NewApiCredentialNotificationServiceClient(deps.Conn),
		searchIndexQueryClient:          searchv1.NewSearchIndexQueryServiceClient(deps.Conn),
		auditEventQueryPort:             newAuditEventQueryPort(deps),
		productVariantValidationClient:  catalogv1.NewProductVariantValidationServiceClient(deps.Conn),
	}
}

// ActivateBrand activates a brand.
func (s *BrandLifecycleServer) ActivateBrand(ctx context.Context, req *catalogv1.ActivateBrandRequest) (*catalogv1.ActivateBrandResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "brand", req.GetBrandId(), "activate_brand"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate brand", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "phone", req.GetDetails().GetNotifyContact().GetPhone())
	shareProductVariantReportRequest := newShareProductVariantReportRequest(withShareProductVariantReportContext(req.GetContext()), withShareProductVariantReportDetails(&catalogv1.ShareProductVariantReportRequest_Details{
		Recipient: &commonv1.ContactInfo{
			Email: req.GetDetails().GetNewOwner().GetEmail(),
			Phone: tenancy.Value(ctx, "phone"),
			Name:  req.GetDetails().GetNotifyContact().GetName(),
		},
	}))
	shareProductVariantReportReply, err := s.productVariantReportingClient.ShareProductVariantReport(ctx, shareProductVariantReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantReportingService/ShareProductVariantReport")
	}
	browseNotificationsRequest := &notificationsv1.BrowseNotificationsRequest{
		Context: req.GetContext(),
		Filter: &notificationsv1.BrowseNotificationsRequest_Filter{
			Region:       shareProductVariantReportReply.GetProductVariant().GetAddress().GetRegion(),
			CreatedAfter: shareProductVariantReportReply.GetProductVariant().GetAudit().GetCreatedAt(),
			OwnerId:      shareProductVariantReportReply.GetProductVariant().GetComponents()["primary"].GetComponentId(),
		},
	}
	browseNotificationsReply, err := grpcx.NewClient(s.conn, notificationsv1.NewNotificationQueryServiceClient).BrowseNotifications(ctx, browseNotificationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationQueryService/BrowseNotifications")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	generateBrandReportRequest := newGenerateBrandReportRequest(withGenerateBrandReportContext(req.GetContext()), withGenerateBrandReportBrandId(req.GetBrandId()), withGenerateBrandReportReason(req.GetReason()), withGenerateBrandReportAnnotations(annotations))
	generateBrandReportReply, err := s.brandReportingClient.GenerateBrandReport(ctx, generateBrandReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandReportingService/GenerateBrandReport")
	}
	streamPayoutChangesStream, err := s.payoutSyncClient.StreamPayoutChanges(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutSyncService/StreamPayoutChanges")
	}
	if err := streamPayoutChangesStream.Send(&paymentsv1.StreamPayoutChangesRequest{
		Context: req.GetContext(),
		Frame: &paymentsv1.StreamPayoutChangesRequest_Change{Change: &paymentsv1.Payout{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: shareProductVariantReportReply.GetProductVariant().GetAudit().GetUpdatedAt(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Recipient: shareProductVariantReportReply.GetProductVariant().GetAddress().GetRecipient(),
			},
			Lines: []*paymentsv1.PayoutLine{&paymentsv1.PayoutLine{
				Quantity: shareProductVariantReportReply.GetProductVariant().GetComponents()["primary"].GetQuantity(),
			}},
		}},
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutSyncService/StreamPayoutChanges")
	}
	if err := streamPayoutChangesStream.CloseSend(); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutSyncService/StreamPayoutChanges")
	}
	acknowledged, err := streamPayoutChangesStream.Recv()
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutSyncService/StreamPayoutChanges")
	}
	_ = acknowledged
	return &catalogv1.ActivateBrandResponse{
		Outcome: &catalogv1.ActivateBrandResponse_Outcome{
			Status:      shareProductVariantReportReply.GetOutcome().GetStatus(),
			Message:     browseNotificationsReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: generateBrandReportReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Brand: &catalogv1.Brand{BrandId: req.GetBrandId()},
	}, nil
}

// SuspendBrand suspends a brand.
func (s *BrandLifecycleServer) SuspendBrand(ctx context.Context, req *catalogv1.SuspendBrandRequest) (*catalogv1.SuspendBrandResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuspendBrand", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "brand", req.GetBrandId(), "suspend_brand"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend brand", "tenant", req.GetContext().GetTenantId())
	compareSearchQueryVersionsRequest := buildCompareSearchQueryVersionsRequestForSuspendBrand(req)
	compareSearchQueryVersionsReply, err := s.searchQueryHistoryClient.CompareSearchQueryVersions(ctx, compareSearchQueryVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryHistoryService/CompareSearchQueryVersions")
	}
	productVariantReporting, ok := s.clients.Get("acme.catalog.v1.ProductVariantReportingService").(catalogv1.ProductVariantReportingServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.catalog.v1.ProductVariantReportingService")
	}
	generateProductVariantReportRequest := &catalogv1.GenerateProductVariantReportRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
	}
	generateProductVariantReportReply, err := productVariantReporting.GenerateProductVariantReport(ctx, generateProductVariantReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantReportingService/GenerateProductVariantReport")
	}
	return &catalogv1.SuspendBrandResponse{
		Outcome: &catalogv1.SuspendBrandResponse_Outcome{
			ReferenceId: compareSearchQueryVersionsReply.GetOutcome().GetReferenceId(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: generateProductVariantReportReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Brand: &catalogv1.Brand{BrandId: req.GetBrandId()},
	}, nil
}

// ReinstateBrand reinstates a brand.
func (s *BrandLifecycleServer) ReinstateBrand(ctx context.Context, req *catalogv1.ReinstateBrandRequest) (*catalogv1.ReinstateBrandResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "brand", req.GetBrandId(), "reinstate_brand"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate brand", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*catalogv1.ReinstateBrandRequest_Details)
	ctx = tenancy.With(ctx, "preferred_channel", req.GetDetails().GetNotifyContact().GetPreferredChannel())
	resumeSavedListWorkflowRequest := &cartv1.ResumeSavedListWorkflowRequest{
		Context: req.GetContext(),
		Details: &cartv1.ResumeSavedListWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Name:             req.GetDetails().GetNotifyContact().GetName(),
				Phone:            cloned.GetNotifyContact().GetPhone(),
				PreferredChannel: tenancy.Value(ctx, "preferred_channel"),
			},
		},
		SavedList: &cartv1.SavedList{
			Counterparty: &cartv1.CartRuleRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	}
	resumeSavedListWorkflowReply, err := grpcx.NewClient(s.conn, cartv1.NewSavedListWorkflowServiceClient).ResumeSavedListWorkflow(ctx, resumeSavedListWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListWorkflowService/ResumeSavedListWorkflow")
	}
	suggestSearchIndexesRequest := &searchv1.SuggestSearchIndexesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.SuggestSearchIndexesRequest_Filter{
			Region:       resumeSavedListWorkflowReply.GetSavedList().GetBillingAddress().GetRegion(),
			CreatedAfter: resumeSavedListWorkflowReply.GetSavedList().GetAudit().GetCreatedAt(),
			Labels:       resumeSavedListWorkflowReply.GetSavedList().GetMetadata(),
			OwnerId:      resumeSavedListWorkflowReply.GetSavedList().GetSavedListId(),
		},
	}
	suggestSearchIndexesReply, err := s.searchIndexQueryPort.SuggestSearchIndexes(ctx, suggestSearchIndexesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexQueryService/SuggestSearchIndexes")
	}
	return &catalogv1.ReinstateBrandResponse{
		Brand: &catalogv1.Brand{
			PrimaryAddress: &commonv1.PostalAddress{
				Region: resumeSavedListWorkflowReply.GetSavedList().GetBillingAddress().GetRegion(),
			},
			BrandId: req.GetBrandId(),
		},
		Result: &commonv1.OperationResult{
			Message: suggestSearchIndexesReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &catalogv1.ReinstateBrandResponse_Outcome{
			Status:      suggestSearchIndexesReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	}, nil
}

// TerminateBrand terminates a brand.
func (s *BrandLifecycleServer) TerminateBrand(ctx context.Context, req *catalogv1.TerminateBrandRequest) (*catalogv1.TerminateBrandResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "brand", req.GetBrandId(), "terminate_brand"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate brand", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNewOwner().GetEmail())
	quoteCategoryRequest := &catalogv1.QuoteCategoryRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Manager: &identityv1.UserRef{
						UserId: req.GetDetails().GetNewOwner().GetUserId(),
						Email:  req.GetDetails().GetNotifyContact().GetEmail(),
					},
				}},
			}},
			Manager: &identityv1.UserRef{Email: tenancy.Value(ctx, "email")},
		},
	}
	quoteCategoryReply, err := s.meteredCategoryCalculation.QuoteCategory(ctx, quoteCategoryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryCalculationService/QuoteCategory")
	}
	return &catalogv1.TerminateBrandResponse{
		Brand: &catalogv1.Brand{
			PrimaryAddress: &commonv1.PostalAddress{
				Region: quoteCategoryReply.GetCategory().GetLocation().GetRegion(),
			},
			Contact: &commonv1.ContactInfo{
				Email: quoteCategoryReply.GetCategory().GetManager().GetEmail(),
			},
			BrandId: req.GetBrandId(),
		},
		Outcome: &catalogv1.TerminateBrandResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RenewBrand renews a brand.
func (s *BrandLifecycleServer) RenewBrand(ctx context.Context, req *catalogv1.RenewBrandRequest) (*catalogv1.RenewBrandResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "brand", req.GetBrandId(), "renew_brand"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew brand", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded catalogv1.RenewBrandRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	streamAttributeMetricsStream, err := s.attributeReportingClient.StreamAttributeMetrics(ctx, &catalogv1.StreamAttributeMetricsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Name:             (&decoded).GetNotifyContact().GetName(),
			PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			Email:            req.GetDetails().GetNewOwner().GetEmail(),
			Phone:            req.GetDetails().GetNotifyContact().GetPhone(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.AttributeReportingService/StreamAttributeMetrics")
	}
	for {
		received, err := streamAttributeMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.catalog.v1.AttributeReportingService/StreamAttributeMetrics")
		}
		_ = received
	}
	detailsCache := cache.New[string, *catalogv1.RenewBrandRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	subscribeApiCredentialUpdatesReply, err := s.apiCredentialNotificationClient.SubscribeApiCredentialUpdates(ctx, &identityv1.SubscribeApiCredentialUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.SubscribeApiCredentialUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Phone:            req.GetDetails().GetNotifyContact().GetPhone(),
				Email:            cachedDetails.GetNewOwner().GetEmail(),
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
				Name:             req.GetDetails().GetNotifyContact().GetName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/SubscribeApiCredentialUpdates")
	}
	previewAttributeRequest := &catalogv1.PreviewAttributeRequest{
		Context: req.GetContext(),
		Attribute: &catalogv1.Attribute{
			Audit: &commonv1.AuditInfo{
				CreatedBy: subscribeApiCredentialUpdatesReply.GetApiCredential().GetAudit().GetCreatedBy(),
			},
		},
		Details: &catalogv1.PreviewAttributeRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name: subscribeApiCredentialUpdatesReply.GetApiCredential().GetContact().GetName(),
			},
		},
	}
	previewAttributeReply, err := grpcx.NewClient(s.conn, catalogv1.NewAttributeValidationServiceClient).PreviewAttribute(ctx, previewAttributeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.AttributeValidationService/PreviewAttribute")
	}
	return &catalogv1.RenewBrandResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: subscribeApiCredentialUpdatesReply.GetResult().GetReferenceIds(),
			Status:       previewAttributeReply.GetResult().GetStatus(),
		},
		Brand:   &catalogv1.Brand{BrandId: req.GetBrandId()},
		Outcome: &catalogv1.RenewBrandResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ExpireBrand expires a brand.
func (s *BrandLifecycleServer) ExpireBrand(ctx context.Context, req *catalogv1.ExpireBrandRequest) (*catalogv1.ExpireBrandResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "brand", req.GetBrandId(), "expire_brand"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire brand", "tenant", req.GetContext().GetTenantId())
	explainNotificationDecisionRequest := &notificationsv1.ExplainNotificationDecisionRequest{
		Context: req.GetContext(),
		Notification: &notificationsv1.Notification{
			Origin: &notificationsv1.Notification_Customer{Customer: &notificationsv1.NotificationTemplateRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			}},
			Requester: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
				Name:             req.GetDetails().GetNotifyContact().GetName(),
			},
		},
		Details: &notificationsv1.ExplainNotificationDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNewOwner().GetEmail()},
		},
	}
	explainNotificationDecisionReply := new(notificationsv1.ExplainNotificationDecisionResponse)
	if err := grpcx.Invoke(ctx, s.conn, notificationsv1.NotificationValidationService_ExplainNotificationDecision_FullMethodName, explainNotificationDecisionRequest, explainNotificationDecisionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationValidationService/ExplainNotificationDecision")
	}
	exportSearchIndexesStream, err := s.searchIndexQueryClient.ExportSearchIndexes(ctx, &searchv1.ExportSearchIndexesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ExportSearchIndexesRequest_Filter{
			CreatedAfter: explainNotificationDecisionReply.GetNotification().GetSla().GetDueAt(),
			OwnerId:      explainNotificationDecisionReply.GetNotification().GetCustomer().GetNotificationTemplateId(),
			Labels:       explainNotificationDecisionReply.GetResult().GetDetails(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexQueryService/ExportSearchIndexes")
	}
	for {
		received, err := exportSearchIndexesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexQueryService/ExportSearchIndexes")
		}
		_ = received
	}
	return &catalogv1.ExpireBrandResponse{
		Brand: &catalogv1.Brand{
			RelationshipManager: &identityv1.UserRef{
				Email: explainNotificationDecisionReply.GetNotification().GetAssignee().GetEmail(),
			},
			BrandId: req.GetBrandId(),
		},
		Outcome: &catalogv1.ExpireBrandResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetBrandLifecycle returns a brand.
func (s *BrandLifecycleServer) GetBrandLifecycle(ctx context.Context, req *catalogv1.GetBrandLifecycleRequest) (*catalogv1.GetBrandLifecycleResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "brand", req.GetBrandId(), "get_brand_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get brand lifecycle", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	recordSearchQueryVersionReply, err := s.searchQueryHistoryClient.RecordSearchQueryVersion(ctx, &searchv1.RecordSearchQueryVersionRequest{
		Context: req.GetContext(),
		Details: &searchv1.RecordSearchQueryVersionRequest_Details{AuthorEmail: req.GetContext().GetActor().GetEmail()},
		SearchQuery: &searchv1.SearchQuery{
			CorrelationId: req.GetBrandId(),
			Detail:        packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryHistoryService/RecordSearchQueryVersion")
	}
	return &catalogv1.GetBrandLifecycleResponse{
		Result: &commonv1.OperationResult{
			Message: recordSearchQueryVersionReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Brand: &catalogv1.Brand{BrandId: req.GetBrandId()},
	}, nil
}

// ScheduleBrandExpiry schedules a brand.
func (s *BrandLifecycleServer) ScheduleBrandExpiry(ctx context.Context, req *catalogv1.ScheduleBrandExpiryRequest) (*catalogv1.ScheduleBrandExpiryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveBrand(ctx, req.GetContext().GetTenantId(), req.GetBrand()); err != nil {
		return nil, grpcx.Wrap(err, "save brand")
	}
	s.log.InfoContext(ctx, "schedule brand expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBrand().GetAudit().GetEtag())
	browseAuditEventsRequest := &auditv1.BrowseAuditEventsRequest{
		Context: req.GetContext(),
		Filter:  &auditv1.BrowseAuditEventsRequest_Filter{Region: req.GetBrand().GetPrimaryAddress().GetRegion()},
	}
	browseAuditEventsReply, err := s.auditEventQueryPort.BrowseAuditEvents(ctx, browseAuditEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventQueryService/BrowseAuditEvents")
	}
	findFixedAssetsRequest := &assetsv1.FindFixedAssetsRequest{
		Context: req.GetContext(),
		Filter: &assetsv1.FindFixedAssetsRequest_Filter{
			Region:       req.GetBrand().GetPrimaryAddress().GetRegion(),
			CreatedAfter: req.GetBrand().GetAudit().GetUpdatedAt(),
			Labels:       browseAuditEventsReply.GetResult().GetDetails(),
		},
	}
	findFixedAssetsReply := new(assetsv1.FindFixedAssetsResponse)
	if err := grpc.Invoke(ctx, "/acme.assets.v1.FixedAssetQueryService/FindFixedAssets", findFixedAssetsRequest, findFixedAssetsReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.assets.v1.FixedAssetQueryService/FindFixedAssets")
	}
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNotify().GetEmail())
	validateProductVariantReply, err := s.productVariantValidationClient.ValidateProductVariant(ctx, &catalogv1.ValidateProductVariantRequest{
		Context: req.GetContext(),
		ProductVariant: &catalogv1.ProductVariant{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetBrand().GetAudit().GetCreatedAt()},
		},
		Details: &catalogv1.ValidateProductVariantRequest_Details{
			Contact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantValidationService/ValidateProductVariant")
	}
	_ = validateProductVariantReply
	return &catalogv1.ScheduleBrandExpiryResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: browseAuditEventsReply.GetResult().GetCorrelationId(),
			Message:       findFixedAssetsReply.GetResult().GetMessage(),
			ReferenceIds:  findFixedAssetsReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		Brand: &catalogv1.Brand{
			BrandId:    req.GetBrandId(),
			Attributes: req.GetBrand().GetAttributes(),
		},
	}, nil
}
