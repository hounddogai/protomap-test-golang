package catalog

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// AttributeSyncServer implements `acme.catalog.v1.AttributeSyncService`.
// Keeps attributes in step with the systems of record in other regions.
type AttributeSyncServer struct {
	catalogv1.UnimplementedAttributeSyncServiceServer
	store                           *Store
	log                             *slog.Logger
	conn                            *grpc.ClientConn
	productVariantCalculationOnce   sync.Once
	productVariantCalculationCached catalogv1.ProductVariantCalculationServiceClient
	documentClient                  documentsv1.DocumentServiceClient
	trackingEventReportingClient    shippingv1.TrackingEventReportingServiceClient
	ledgerAccountClient             ledgerv1.LedgerAccountServiceClient
	productVariantReportingClient   catalogv1.ProductVariantReportingServiceClient
	paymentInstrumentApprovalClient paymentsv1.PaymentInstrumentApprovalServiceClient
	categoryCalculationClient       catalogv1.CategoryCalculationServiceClient
	searchIndexNotificationClient   searchv1.SearchIndexNotificationServiceClient
	meteredBrandValidation          meteredBrandValidation
	productVariantValidationClient  catalogv1.ProductVariantValidationServiceClient
	productVariantCalculationClient catalogv1.ProductVariantCalculationServiceClient
	startSubscriptionWorkflowFn     func(context.Context, *subscriptionsv1.StartSubscriptionWorkflowRequest, ...grpc.CallOption) (*subscriptionsv1.StartSubscriptionWorkflowResponse, error)
}

// NewAttributeSyncServer wires an AttributeSyncServer.
func NewAttributeSyncServer(deps platform.Deps) *AttributeSyncServer {
	subscriptionWorkflow := subscriptionsv1.NewSubscriptionWorkflowServiceClient(deps.Conn)
	return &AttributeSyncServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.catalog.v1.AttributeSyncService"),
		conn:                            deps.Conn,
		documentClient:                  documentsv1.NewDocumentServiceClient(deps.Conn),
		trackingEventReportingClient:    shippingv1.NewTrackingEventReportingServiceClient(deps.Conn),
		ledgerAccountClient:             ledgerv1.NewLedgerAccountServiceClient(deps.Conn),
		productVariantReportingClient:   catalogv1.NewProductVariantReportingServiceClient(deps.Conn),
		paymentInstrumentApprovalClient: paymentsv1.NewPaymentInstrumentApprovalServiceClient(deps.Conn),
		categoryCalculationClient:       catalogv1.NewCategoryCalculationServiceClient(deps.Conn),
		searchIndexNotificationClient:   searchv1.NewSearchIndexNotificationServiceClient(deps.Conn),
		meteredBrandValidation:          meteredBrandValidation{BrandValidationServiceClient: catalogv1.NewBrandValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		productVariantValidationClient:  catalogv1.NewProductVariantValidationServiceClient(deps.Conn),
		productVariantCalculationClient: catalogv1.NewProductVariantCalculationServiceClient(deps.Conn),
		startSubscriptionWorkflowFn:     subscriptionWorkflow.StartSubscriptionWorkflow,
	}
}

// SyncAttributes syncs an attribute.
func (s *AttributeSyncServer) SyncAttributes(ctx context.Context, req *catalogv1.SyncAttributesRequest) (*catalogv1.SyncAttributesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveAttribute(ctx, req.GetContext().GetTenantId(), req.GetAttribute()); err != nil {
		return nil, grpcx.Wrap(err, "save attribute")
	}
	s.log.InfoContext(ctx, "sync attributes", "tenant", req.GetContext().GetTenantId(), "etag", req.GetAttribute().GetAudit().GetEtag())
	source := req.GetAttribute().GetAudit().ProtoReflect()
	etag := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("etag"))).String()
	simulateProductVariantRequest := &catalogv1.SimulateProductVariantRequest{
		Context: req.GetContext(),
		ProductVariant: &catalogv1.ProductVariant{
			Owner: &identityv1.UserRef{
				UserId:      req.GetAttribute().GetOwner().GetUserId(),
				DisplayName: req.GetAttribute().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{Etag: etag},
		},
	}
	simulateProductVariantReply, err := s.productVariantCalculationLazy().SimulateProductVariant(ctx, simulateProductVariantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantCalculationService/SimulateProductVariant")
	}
	watchDocumentsStream, err := s.documentClient.WatchDocuments(ctx, &documentsv1.WatchDocumentsRequest{
		Context: req.GetContext(),
		Filter: &documentsv1.WatchDocumentsRequest_Filter{
			Region:       simulateProductVariantReply.GetProductVariant().GetAddress().GetRegion(),
			CreatedAfter: simulateProductVariantReply.GetProductVariant().GetAudit().GetCreatedAt(),
		},
		Subscriber: &commonv1.ContactInfo{
			Email: simulateProductVariantReply.GetProductVariant().GetOwner().GetEmail(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentService/WatchDocuments")
	}
	for {
		received, err := watchDocumentsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentService/WatchDocuments")
		}
		_ = received
	}
	return &catalogv1.SyncAttributesResponse{
		Attribute: &catalogv1.Attribute{
			Name: simulateProductVariantReply.GetProductVariant().GetName(),
			Audit: &commonv1.AuditInfo{
				UpdatedBy: simulateProductVariantReply.GetProductVariant().GetAudit().GetUpdatedBy(),
			},
			AttributeId: req.GetAttributeId(),
			Description: req.GetAttribute().GetDescription(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PushAttribute pushs an attribute.
func (s *AttributeSyncServer) PushAttribute(ctx context.Context, req *catalogv1.PushAttributeRequest) (*catalogv1.PushAttributeResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PushAttribute", "error", err)
		return nil, err
	}
	if err := s.store.SaveAttribute(ctx, req.GetContext().GetTenantId(), req.GetAttribute()); err != nil {
		return nil, grpcx.Wrap(err, "save attribute")
	}
	s.log.InfoContext(ctx, "push attribute", "tenant", req.GetContext().GetTenantId(), "etag", req.GetAttribute().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	var reason string
	switch choice := req.GetAttribute().GetAction().(type) {
	case *catalogv1.Attribute_Route:
		reason = choice.Route.GetTarget()
	case *catalogv1.Attribute_Block:
		reason = choice.Block.GetReason()
	}
	scheduleTrackingEventReportRequest := &shippingv1.ScheduleTrackingEventReportRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      reason,
	}
	scheduleTrackingEventReportReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*shippingv1.ScheduleTrackingEventReportResponse, error) {
		return s.trackingEventReportingClient.ScheduleTrackingEventReport(ctx, scheduleTrackingEventReportRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventReportingService/ScheduleTrackingEventReport")
	}
	recordUserVersionRequest := &identityv1.RecordUserVersionRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetAttribute().GetAudit().GetCreatedBy(),
				CreatedAt: req.GetAttribute().GetAudit().GetCreatedAt(),
			},
			RelationshipManager: &identityv1.UserRef{Email: req.GetAttribute().GetOwner().GetEmail()},
		},
	}
	recordUserVersionReply := new(identityv1.RecordUserVersionResponse)
	if err := grpcx.Invoke(ctx, s.conn, identityv1.UserHistoryService_RecordUserVersion_FullMethodName, recordUserVersionRequest, recordUserVersionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserHistoryService/RecordUserVersion")
	}
	createLedgerAccountRequest := &ledgerv1.CreateLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Limits: []*ledgerv1.LedgerAccountLimit{&ledgerv1.LedgerAccountLimit{Amount: req.GetAttribute().GetDiscount().GetAmount()}},
			Contact: &commonv1.ContactInfo{
				Phone: recordUserVersionReply.GetUser().GetContact().GetPhone(),
			},
			Holder: &ledgerv1.PostingBatchRef{
				Number: recordUserVersionReply.GetUser().GetNationalId().GetNumber(),
			},
		},
	}
	createLedgerAccountReply, err := ledgerv1.LedgerAccountServiceClient.CreateLedgerAccount(s.ledgerAccountClient, ctx, createLedgerAccountRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/CreateLedgerAccount")
	}
	_ = createLedgerAccountReply
	return &catalogv1.PushAttributeResponse{
		Outcome: &catalogv1.PushAttributeResponse_Outcome{
			Status: scheduleTrackingEventReportReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: scheduleTrackingEventReportReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Attribute: &catalogv1.Attribute{
			AttributeId: req.GetAttributeId(),
			Effective:   req.GetAttribute().GetEffective(),
		},
	}, nil
}

// PullAttributes pulls an attribute.
func (s *AttributeSyncServer) PullAttributes(ctx context.Context, req *catalogv1.PullAttributesRequest) (*catalogv1.PullAttributesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PullAttributes", "error", err)
		return nil, err
	}
	if err := s.store.SaveAttribute(ctx, req.GetContext().GetTenantId(), req.GetAttribute()); err != nil {
		return nil, grpcx.Wrap(err, "save attribute")
	}
	s.log.InfoContext(ctx, "pull attributes", "tenant", req.GetContext().GetTenantId(), "etag", req.GetAttribute().GetAudit().GetEtag())
	var reason string
	switch choice := req.GetAttribute().GetAction().(type) {
	case *catalogv1.Attribute_Route:
		reason = choice.Route.GetTarget()
	case *catalogv1.Attribute_Block:
		reason = choice.Block.GetReason()
	}
	generateProductVariantReportReply, err := s.productVariantReportingClient.GenerateProductVariantReport(ctx, &catalogv1.GenerateProductVariantReportRequest{
		Context: req.GetContext(),
		Reason:  reason,
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantReportingService/GenerateProductVariantReport")
	}
	listPendingPaymentInstrumentsRequest := &paymentsv1.ListPendingPaymentInstrumentsRequest{
		Context: req.GetContext(),
		Filter: &paymentsv1.ListPendingPaymentInstrumentsRequest_Filter{
			CreatedAfter: req.GetAttribute().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetAttribute().GetAttributeId(),
			Labels:       generateProductVariantReportReply.GetResult().GetDetails(),
		},
	}
	listPendingPaymentInstrumentsReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*paymentsv1.ListPendingPaymentInstrumentsResponse, error) {
		return s.paymentInstrumentApprovalClient.ListPendingPaymentInstruments(ctx, listPendingPaymentInstrumentsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentApprovalService/ListPendingPaymentInstruments")
	}
	return &catalogv1.PullAttributesResponse{
		Result: &commonv1.OperationResult{
			Status:  generateProductVariantReportReply.GetResult().GetStatus(),
			Details: generateProductVariantReportReply.GetResult().GetDetails(),
			Message: listPendingPaymentInstrumentsReply.GetResult().GetMessage(),
		},
		Attribute: &catalogv1.Attribute{
			AttributeId: req.GetAttributeId(),
			Description: req.GetAttribute().GetDescription(),
		},
	}, nil
}

// StreamAttributeChanges exchanges changes of attributes with a peer.
func (s *AttributeSyncServer) StreamAttributeChanges(stream grpc.BidiStreamingServer[catalogv1.StreamAttributeChangesRequest, catalogv1.StreamAttributeChangesResponse]) error {
	ctx := stream.Context()
	incoming := make(chan *catalogv1.StreamAttributeChangesRequest)
	failed := make(chan error, 1)
	go func() {
		defer close(incoming)
		for {
			received, err := stream.Recv()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					failed <- err
				}
				return
			}
			select {
			case incoming <- received:
			case <-ctx.Done():
				return
			}
		}
	}()
	for msg := range incoming {
		s.log.InfoContext(ctx, "stream attribute changes", "tenant", msg.GetContext().GetTenantId())
		quoteCategoryRequest := &catalogv1.QuoteCategoryRequest{
			Context: msg.GetContext(),
			Category: &catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Children: []*catalogv1.Category{&catalogv1.Category{
						Audit:    &commonv1.AuditInfo{CreatedAt: msg.GetChange().GetAudit().GetCreatedAt()},
						Children: []*catalogv1.Category{&catalogv1.Category{Name: msg.GetChange().GetName()}},
					}},
					Manager: &identityv1.UserRef{UserId: msg.GetChange().GetOwner().GetUserId()},
				}},
			},
		}
		if _, err := grpcx.Call(ctx, s.categoryCalculationClient.QuoteCategory, quoteCategoryRequest); err != nil {
			return grpcx.Wrap(err, "acme.catalog.v1.CategoryCalculationService/QuoteCategory")
		}
		if _, err := s.searchIndexNotificationClient.SubscribeSearchIndexUpdates(ctx, &searchv1.SubscribeSearchIndexUpdatesRequest{
			Context: msg.GetContext(),
			Details: &searchv1.SubscribeSearchIndexUpdatesRequest_Details{
				Recipient: &commonv1.ContactInfo{Email: msg.GetChange().GetOwner().GetEmail()},
			},
		}); err != nil {
			return grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/SubscribeSearchIndexUpdates")
		}
		if _, err := s.trackingEventReportingClient.ShareTrackingEventReport(ctx, &shippingv1.ShareTrackingEventReportRequest{
			Context: msg.GetContext(),
			Reason:  msg.GetChange().GetBlock().GetReason(),
		}); err != nil {
			return grpcx.Wrap(err, "acme.shipping.v1.TrackingEventReportingService/ShareTrackingEventReport")
		}
		normalizeBrandRequest := &catalogv1.NormalizeBrandRequest{
			Context: msg.GetContext(),
			Brand: &catalogv1.Brand{
				RelationshipManager: &identityv1.UserRef{DisplayName: msg.GetChange().GetOwner().GetDisplayName()},
				Audit:               &commonv1.AuditInfo{CreatedBy: msg.GetChange().GetAudit().GetCreatedBy()},
			},
		}
		if _, err := s.meteredBrandValidation.NormalizeBrand(ctx, normalizeBrandRequest); err != nil {
			return grpcx.Wrap(err, "acme.catalog.v1.BrandValidationService/NormalizeBrand")
		}
		if err := stream.Send(&catalogv1.StreamAttributeChangesResponse{Result: &commonv1.OperationResult{Status: "ok"}}); err != nil {
			return err
		}
	}
	select {
	case err := <-failed:
		return err
	default:
		return nil
	}
}

// ResolveAttributeConflict resolves an attribute.
func (s *AttributeSyncServer) ResolveAttributeConflict(ctx context.Context, req *catalogv1.ResolveAttributeConflictRequest) (*catalogv1.ResolveAttributeConflictResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveAttribute(ctx, req.GetContext().GetTenantId(), req.GetAttribute()); err != nil {
		return nil, grpcx.Wrap(err, "save attribute")
	}
	s.log.InfoContext(ctx, "resolve attribute conflict", "tenant", req.GetContext().GetTenantId(), "etag", req.GetAttribute().GetAudit().GetEtag())
	lintProductVariantsReply, err := s.productVariantValidationClient.LintProductVariants(ctx, &catalogv1.LintProductVariantsRequest{
		Context: req.GetContext(),
		ProductVariant: &catalogv1.ProductVariant{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetAttribute().GetOwner().GetDisplayName(),
				UserId:      req.GetAttribute().GetOwner().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetAttribute().GetAudit().GetEtag(),
				UpdatedAt: req.GetAttribute().GetAudit().GetUpdatedAt(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantValidationService/LintProductVariants")
	}
	simulateProductVariantRequest := newSimulateProductVariantRequest(withSimulateProductVariantContext(req.GetContext()), withSimulateProductVariantProductVariant(&catalogv1.ProductVariant{
		Owner: &identityv1.UserRef{
			DisplayName: lintProductVariantsReply.GetProductVariant().GetOwner().GetDisplayName(),
		},
	}))
	simulateProductVariantReply, err := s.productVariantCalculationClient.SimulateProductVariant(ctx, simulateProductVariantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantCalculationService/SimulateProductVariant")
	}
	return &catalogv1.ResolveAttributeConflictResponse{
		Attribute: &catalogv1.Attribute{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: lintProductVariantsReply.GetProductVariant().GetAudit().GetUpdatedAt(),
			},
			AttributeId: req.GetAttributeId(),
			Name:        req.GetAttribute().GetName(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: simulateProductVariantReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// GetAttributeSyncState returns an attribute.
func (s *AttributeSyncServer) GetAttributeSyncState(ctx context.Context, req *catalogv1.GetAttributeSyncStateRequest) (*catalogv1.GetAttributeSyncStateResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "attribute", req.GetAttributeId(), "get_attribute_sync_state"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get attribute sync state", "tenant", req.GetContext().GetTenantId())
	startSubscriptionWorkflowRequest := &subscriptionsv1.StartSubscriptionWorkflowRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.StartSubscriptionWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	startSubscriptionWorkflowReply, err := s.startSubscriptionWorkflowFn(ctx, startSubscriptionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionWorkflowService/StartSubscriptionWorkflow")
	}
	return &catalogv1.GetAttributeSyncStateResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: startSubscriptionWorkflowReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Attribute: &catalogv1.Attribute{
			Audit: &commonv1.AuditInfo{
				Etag: startSubscriptionWorkflowReply.GetSubscription().GetAudit().GetEtag(),
			},
			AttributeId: req.GetAttributeId(),
		},
	}, nil
}

// ListAttributeConflicts lists attributes a page at a time.
func (s *AttributeSyncServer) ListAttributeConflicts(ctx context.Context, req *catalogv1.ListAttributeConflictsRequest) (*catalogv1.ListAttributeConflictsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListAttributeConflicts", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list attribute conflicts", "tenant", req.GetContext().GetTenantId())
	filterCache := cache.New[string, *catalogv1.ListAttributeConflictsRequest_Filter](1)
	filterCache.Put(req.GetContext().GetTraceId(), req.GetFilter())
	cachedFilter, _ := filterCache.Get(req.GetContext().GetTraceId())
	countTrackingEventTotalsRequest := &shippingv1.CountTrackingEventTotalsRequest{
		Context: req.GetContext(),
		Filter: &shippingv1.CountTrackingEventTotalsRequest_Filter{
			OwnerId: cachedFilter.GetOwnerId(),
			Region:  req.GetFilter().GetRegion(),
		},
	}
	countTrackingEventTotalsReply, err := shippingv1.TrackingEventReportingServiceClient.CountTrackingEventTotals(s.trackingEventReportingClient, ctx, countTrackingEventTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventReportingService/CountTrackingEventTotals")
	}
	return &catalogv1.ListAttributeConflictsResponse{
		Result: &commonv1.OperationResult{
			Message: countTrackingEventTotalsReply.GetResult().GetMessage(),
			Status:  countTrackingEventTotalsReply.GetResult().GetStatus(),
		},
	}, nil
}

// productVariantCalculationLazy dials acme.catalog.v1.ProductVariantCalculationService the first time a handler needs
// it.
func (s *AttributeSyncServer) productVariantCalculationLazy() catalogv1.ProductVariantCalculationServiceClient {
	s.productVariantCalculationOnce.Do(func() {
		s.productVariantCalculationCached = catalogv1.NewProductVariantCalculationServiceClient(s.conn)
	})
	return s.productVariantCalculationCached
}
