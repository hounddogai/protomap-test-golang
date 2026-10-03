package catalog

import (
	"context"
	"errors"
	"io"
	"log/slog"

	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	ratelimitingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ratelimiting/v1"
	seatingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/seating/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// CategoryLifecycleServer implements `acme.catalog.v1.CategoryLifecycleService`.
// Moves a category between the states of its lifecycle.
type CategoryLifecycleServer struct {
	catalogv1.UnimplementedCategoryLifecycleServiceServer
	store                           *Store
	log                             *slog.Logger
	fraudSignalLifecycleClient      fraudv1.FraudSignalLifecycleServiceClient
	productVariantValidationClient  catalogv1.ProductVariantValidationServiceClient
	categoryQueryClient             catalogv1.CategoryQueryServiceClient
	venueValidationClient           venuesv1.VenueValidationServiceClient
	venueValidationClientFallback   venuesv1.VenueValidationServiceClient
	findVenuesFn                    func(context.Context, *venuesv1.FindVenuesRequest, ...grpc.CallOption) (*venuesv1.FindVenuesResponse, error)
	featureFlagValidationClient     featureflagsv1.FeatureFlagValidationServiceClient
	conn                            *grpc.ClientConn
	notificationClient              notificationsv1.NotificationServiceClient
	sessionQueryClient              identityv1.SessionQueryServiceClient
	seatMapCalculationClient        seatingv1.SeatMapCalculationServiceClient
	stockMovementWorkflowClient     inventoryv1.StockMovementWorkflowServiceClient
	rateLimitClient                 ratelimitingv1.RateLimitServiceClient
	productVariantCalculationClient catalogv1.ProductVariantCalculationServiceClient
	attributeCalculationClient      catalogv1.AttributeCalculationServiceClient
}

// NewCategoryLifecycleServer wires a CategoryLifecycleServer.
func NewCategoryLifecycleServer(deps platform.Deps) *CategoryLifecycleServer {
	venueQuery := venuesv1.NewVenueQueryServiceClient(deps.Conn)
	return &CategoryLifecycleServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.catalog.v1.CategoryLifecycleService"),
		fraudSignalLifecycleClient:      fraudv1.NewFraudSignalLifecycleServiceClient(deps.Conn),
		productVariantValidationClient:  catalogv1.NewProductVariantValidationServiceClient(deps.Conn),
		categoryQueryClient:             catalogv1.NewCategoryQueryServiceClient(deps.Conn),
		venueValidationClient:           venuesv1.NewVenueValidationServiceClient(deps.Conn),
		venueValidationClientFallback:   venuesv1.NewVenueValidationServiceClient(deps.Conn),
		findVenuesFn:                    venueQuery.FindVenues,
		featureFlagValidationClient:     featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn),
		conn:                            deps.Conn,
		notificationClient:              notificationsv1.NewNotificationServiceClient(deps.Conn),
		sessionQueryClient:              identityv1.NewSessionQueryServiceClient(deps.Conn),
		seatMapCalculationClient:        seatingv1.NewSeatMapCalculationServiceClient(deps.Conn),
		stockMovementWorkflowClient:     inventoryv1.NewStockMovementWorkflowServiceClient(deps.Conn),
		rateLimitClient:                 ratelimitingv1.NewRateLimitServiceClient(deps.Conn),
		productVariantCalculationClient: catalogv1.NewProductVariantCalculationServiceClient(deps.Conn),
		attributeCalculationClient:      catalogv1.NewAttributeCalculationServiceClient(deps.Conn),
	}
}

// ActivateCategory activates a category.
func (s *CategoryLifecycleServer) ActivateCategory(ctx context.Context, req *catalogv1.ActivateCategoryRequest) (*catalogv1.ActivateCategoryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ActivateCategory", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "category", req.GetCategoryId(), "activate_category"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate category", "tenant", req.GetContext().GetTenantId())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := categoryLifecycleServerScheduleFraudSignalExpirySteps[mode]
	scheduleFraudSignalExpiryRequest := &fraudv1.ScheduleFraudSignalExpiryRequest{
		Context: req.GetContext(),
		Details: &fraudv1.ScheduleFraudSignalExpiryRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetDetails().GetNewOwner().GetEmail()},
		},
	}
	scheduleFraudSignalExpiryReply, err := run(s, ctx, scheduleFraudSignalExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalLifecycleService/ScheduleFraudSignalExpiry")
	}
	return &catalogv1.ActivateCategoryResponse{
		Outcome: &catalogv1.ActivateCategoryResponse_Outcome{
			ReferenceId: scheduleFraudSignalExpiryReply.GetOutcome().GetReferenceId(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Message: scheduleFraudSignalExpiryReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Category: &catalogv1.Category{CategoryId: req.GetCategoryId()},
	}, nil
}

// SuspendCategory suspends a category.
func (s *CategoryLifecycleServer) SuspendCategory(ctx context.Context, req *catalogv1.SuspendCategoryRequest) (*catalogv1.SuspendCategoryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "category", req.GetCategoryId(), "suspend_category"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend category", "tenant", req.GetContext().GetTenantId())
	validateProductVariantRequest := &catalogv1.ValidateProductVariantRequest{}
	validateProductVariantRequest.Context = req.GetContext()
	validateProductVariantRequest.Details = &catalogv1.ValidateProductVariantRequest_Details{
		Contact: &commonv1.ContactInfo{
			Name:             req.GetDetails().GetNotifyContact().GetName(),
			Phone:            req.GetDetails().GetNotifyContact().GetPhone(),
			PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
		},
	}
	validateProductVariantReply, err := s.productVariantValidationClient.ValidateProductVariant(ctx, validateProductVariantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantValidationService/ValidateProductVariant")
	}
	peekCategoryRequest := buildPeekCategoryRequestForSuspendCategory(req)
	peekCategoryReply, err := s.categoryQueryClient.PeekCategory(ctx, peekCategoryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryQueryService/PeekCategory")
	}
	return &catalogv1.SuspendCategoryResponse{
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Manager: &identityv1.UserRef{
					Email: validateProductVariantReply.GetProductVariant().GetOwner().GetEmail(),
				},
				CategoryId: peekCategoryReply.GetCategory().GetCategoryId(),
			}},
			CategoryId: req.GetCategoryId(),
		},
		Outcome: &catalogv1.SuspendCategoryResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ReinstateCategory reinstates a category.
func (s *CategoryLifecycleServer) ReinstateCategory(ctx context.Context, req *catalogv1.ReinstateCategoryRequest) (*catalogv1.ReinstateCategoryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "category", req.GetCategoryId(), "reinstate_category"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate category", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *venuesv1.ValidateVenueRequest, ...grpc.CallOption) (*venuesv1.ValidateVenueResponse, error){
		"primary":  s.venueValidationClient.ValidateVenue,
		"fallback": s.venueValidationClientFallback.ValidateVenue,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	validateVenueRequest := &venuesv1.ValidateVenueRequest{
		Context: req.GetContext(),
		Details: &venuesv1.ValidateVenueRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNewOwner().GetEmail()},
		},
	}
	validateVenueReply, err := route(ctx, validateVenueRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueValidationService/ValidateVenue")
	}
	findVenuesRequest := &venuesv1.FindVenuesRequest{
		Context: req.GetContext(),
		Filter: &venuesv1.FindVenuesRequest_Filter{
			Region: validateVenueReply.GetVenue().GetAddress().GetRegion(),
		},
	}
	findVenuesReply, err := s.findVenuesFn(ctx, findVenuesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueQueryService/FindVenues")
	}
	return &catalogv1.ReinstateCategoryResponse{
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{Name: validateVenueReply.GetVenue().GetName()}},
			}},
			CategoryId: req.GetCategoryId(),
		},
		Outcome: &catalogv1.ReinstateCategoryResponse_Outcome{
			Status:      findVenuesReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Details: findVenuesReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// TerminateCategory terminates a category.
func (s *CategoryLifecycleServer) TerminateCategory(ctx context.Context, req *catalogv1.TerminateCategoryRequest) (*catalogv1.TerminateCategoryResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "category", req.GetCategoryId(), "terminate_category"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate category", "tenant", req.GetContext().GetTenantId())
	normalizeFeatureFlagRequest := buildNormalizeFeatureFlagRequestForTerminateCategory(req)
	normalizeFeatureFlagReply, err := s.featureFlagValidationClient.NormalizeFeatureFlag(ctx, normalizeFeatureFlagRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/NormalizeFeatureFlag")
	}
	previewCategoryRequest := &catalogv1.PreviewCategoryRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Manager: &identityv1.UserRef{Email: req.GetDetails().GetNewOwner().GetEmail()},
					Audit: &commonv1.AuditInfo{
						UpdatedBy: normalizeFeatureFlagReply.GetFeatureFlag().GetAudit().GetUpdatedBy(),
						CreatedAt: normalizeFeatureFlagReply.GetFeatureFlag().GetAudit().GetCreatedAt(),
					},
				}},
				Manager: &identityv1.UserRef{Email: req.GetDetails().GetNotifyContact().GetEmail()},
			}},
		},
	}
	previewCategoryReply := new(catalogv1.PreviewCategoryResponse)
	if err := grpcx.Invoke(ctx, s.conn, catalogv1.CategoryValidationService_PreviewCategory_FullMethodName, previewCategoryRequest, previewCategoryReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryValidationService/PreviewCategory")
	}
	return &catalogv1.TerminateCategoryResponse{
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Audit: &commonv1.AuditInfo{
						CreatedBy: normalizeFeatureFlagReply.GetFeatureFlag().GetAudit().GetCreatedBy(),
					},
					Children: []*catalogv1.Category{&catalogv1.Category{Path: previewCategoryReply.GetCategory().GetPath()}},
					Location: previewCategoryReply.GetCategory().GetLocation(),
				}},
			}},
			CategoryId: req.GetCategoryId(),
		},
		Outcome: &catalogv1.TerminateCategoryResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RenewCategory renews a category.
func (s *CategoryLifecycleServer) RenewCategory(ctx context.Context, req *catalogv1.RenewCategoryRequest) (*catalogv1.RenewCategoryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RenewCategory", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "category", req.GetCategoryId(), "renew_category"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew category", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "preferred_channel", req.GetDetails().GetNotifyContact().GetPreferredChannel())
	cloned := proto.Clone(req.GetDetails()).(*catalogv1.RenewCategoryRequest_Details)
	createNotificationRequest := &notificationsv1.CreateNotificationRequest{
		Context: req.GetContext(),
		Notification: &notificationsv1.Notification{
			Requester: &commonv1.ContactInfo{
				PreferredChannel: tenancy.Value(ctx, "preferred_channel"),
				Name:             cloned.GetNotifyContact().GetName(),
				Phone:            req.GetDetails().GetNotifyContact().GetPhone(),
			},
			Assignee: &identityv1.UserRef{Email: req.GetDetails().GetNewOwner().GetEmail()},
		},
	}
	send := s.createNotificationSender(createNotificationRequest)
	createNotificationReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationService/CreateNotification")
	}
	exportSessionsStream, err := s.sessionQueryClient.ExportSessions(ctx, &identityv1.ExportSessionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ExportSessionsRequest_Filter{
			CreatedAfter: createNotificationReply.GetNotification().GetAudit().GetCreatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/ExportSessions")
	}
	for {
		received, err := exportSessionsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/ExportSessions")
		}
		_ = received
	}
	quoteSeatMapReply, err := s.seatMapCalculationClient.QuoteSeatMap(ctx, &seatingv1.QuoteSeatMapRequest{
		Context: req.GetContext(),
		SeatMap: &seatingv1.SeatMap{
			Children: []*seatingv1.SeatMap{&seatingv1.SeatMap{
				Children: []*seatingv1.SeatMap{&seatingv1.SeatMap{
					Audit: &commonv1.AuditInfo{
						CreatedAt: createNotificationReply.GetNotification().GetAudit().GetCreatedAt(),
					},
					Manager: &identityv1.UserRef{
						UserId: createNotificationReply.GetNotification().GetAssignee().GetUserId(),
					},
				}},
				Manager: &identityv1.UserRef{
					DisplayName: createNotificationReply.GetNotification().GetCustomer().GetDisplayName(),
				},
				Audit: &commonv1.AuditInfo{
					UpdatedBy: createNotificationReply.GetNotification().GetAudit().GetUpdatedBy(),
				},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.seating.v1.SeatMapCalculationService/QuoteSeatMap")
	}
	detailsCache := cache.New[string, *catalogv1.RenewCategoryRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	step := &categoryLifecycleServerRenewCategoryRetryStockMovementWorkflowStep{request: &inventoryv1.RetryStockMovementWorkflowRequest{
		Context: req.GetContext(),
		StockMovement: &inventoryv1.StockMovement{
			Subject: &customersv1.CustomerRef{
				DisplayName: quoteSeatMapReply.GetSeatMap().GetManager().GetDisplayName(),
				Email:       req.GetDetails().GetNewOwner().GetEmail(),
			},
		},
		Details: &inventoryv1.RetryStockMovementWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Name:  cachedDetails.GetNotifyContact().GetName(),
				Phone: req.GetDetails().GetNotifyContact().GetPhone(),
			},
		},
	}}
	if err := s.runRenewCategoryRetryStockMovementWorkflow(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockMovementWorkflowService/RetryStockMovementWorkflow")
	}
	retryStockMovementWorkflowReply := step.reply
	return &catalogv1.RenewCategoryResponse{
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Manager: &identityv1.UserRef{
						DisplayName: createNotificationReply.GetNotification().GetAssignee().GetDisplayName(),
					},
					Children: []*catalogv1.Category{&catalogv1.Category{Depth: quoteSeatMapReply.GetSeatMap().GetDepth()}},
				}},
			}},
			Attributes: retryStockMovementWorkflowReply.GetStockMovement().GetAttributes(),
			CategoryId: req.GetCategoryId(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: retryStockMovementWorkflowReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &catalogv1.RenewCategoryResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ExpireCategory expires a category.
func (s *CategoryLifecycleServer) ExpireCategory(ctx context.Context, req *catalogv1.ExpireCategoryRequest) (*catalogv1.ExpireCategoryResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "category", req.GetCategoryId(), "expire_category"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire category", "tenant", req.GetContext().GetTenantId())
	cloneRateLimitRequest := &ratelimitingv1.CloneRateLimitRequest{
		Context: req.GetContext(),
		RateLimit: &ratelimitingv1.RateLimit{
			Owner: &identityv1.UserRef{
				Email:       req.GetDetails().GetNotifyContact().GetEmail(),
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
				UserId:      req.GetDetails().GetNewOwner().GetUserId(),
			},
		},
	}
	send := s.cloneRateLimitSender(cloneRateLimitRequest)
	cloneRateLimitReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ratelimiting.v1.RateLimitService/CloneRateLimit")
	}
	signalPaymentInstrumentWorkflowRequest := &paymentsv1.SignalPaymentInstrumentWorkflowRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Contact: &commonv1.ContactInfo{
				Email: req.GetDetails().GetNewOwner().GetEmail(),
				Name:  req.GetDetails().GetNotifyContact().GetName(),
			},
			Holder: &customersv1.CustomerRef{
				DisplayName: cloneRateLimitReply.GetRateLimit().GetOwner().GetDisplayName(),
			},
		},
		Details: &paymentsv1.SignalPaymentInstrumentWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: cloneRateLimitReply.GetRateLimit().GetOwner().GetEmail(),
			},
		},
	}
	signalPaymentInstrumentWorkflowReply, err := signalPaymentInstrumentWorkflowCall(ctx, signalPaymentInstrumentWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentWorkflowService/SignalPaymentInstrumentWorkflow")
	}
	return &catalogv1.ExpireCategoryResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: cloneRateLimitReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Category: &catalogv1.Category{
			Audit: &commonv1.AuditInfo{
				CreatedBy: cloneRateLimitReply.GetRateLimit().GetAudit().GetCreatedBy(),
			},
			Children: []*catalogv1.Category{&catalogv1.Category{
				Manager: &identityv1.UserRef{
					Email: signalPaymentInstrumentWorkflowReply.GetPaymentInstrument().GetContact().GetEmail(),
				},
			}},
			CategoryId: req.GetCategoryId(),
		},
		Outcome: &catalogv1.ExpireCategoryResponse_Outcome{
			ReferenceId: signalPaymentInstrumentWorkflowReply.GetOutcome().GetReferenceId(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	}, nil
}

// ScheduleCategoryExpiry schedules a category.
func (s *CategoryLifecycleServer) ScheduleCategoryExpiry(ctx context.Context, req *catalogv1.ScheduleCategoryExpiryRequest) (*catalogv1.ScheduleCategoryExpiryResponse, error) {
	if err := s.store.SaveCategory(ctx, req.GetContext().GetTenantId(), req.GetCategory()); err != nil {
		return nil, grpcx.Wrap(err, "save category")
	}
	s.log.InfoContext(ctx, "schedule category expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCategory().GetAudit().GetEtag())
	names := collectCategoryNames(req.GetCategory(), nil)
	recalculateProductVariantsRequest := &catalogv1.RecalculateProductVariantsRequest{
		Context: req.GetContext(),
		Details: &catalogv1.RecalculateProductVariantsRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: req.GetCategory().GetLocation().GetCountryCode(),
			},
		},
		ProductVariant: &catalogv1.ProductVariant{
			Owner:        &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
			CategoryPath: names,
		},
	}
	recalculateProductVariantsReply, err := catalogv1.ProductVariantCalculationServiceClient.RecalculateProductVariants(s.productVariantCalculationClient, ctx, recalculateProductVariantsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantCalculationService/RecalculateProductVariants")
	}
	lintProductsRequest := &catalogv1.LintProductsRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Dimensions: &catalogv1.ProductDimensions{
				Height: recalculateProductVariantsReply.GetProductVariant().GetDimensions().GetHeight(),
			},
		},
	}
	lintProductsReply := new(catalogv1.LintProductsResponse)
	if err := grpcx.Invoke(ctx, s.conn, catalogv1.ProductValidationService_LintProducts_FullMethodName, lintProductsRequest, lintProductsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductValidationService/LintProducts")
	}
	calculateAttributeReply, err := s.attributeCalculationClient.CalculateAttribute(ctx, &catalogv1.CalculateAttributeRequest{
		Context: req.GetContext(),
		Attribute: &catalogv1.Attribute{
			Owner: &identityv1.UserRef{
				DisplayName: lintProductsReply.GetProduct().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetCategory().GetAudit().GetUpdatedBy()},
		},
		Details: &catalogv1.CalculateAttributeRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: recalculateProductVariantsReply.GetProductVariant().GetAddress().GetPostalCode(),
				Recipient:  lintProductsReply.GetProduct().GetAddress().GetRecipient(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.AttributeCalculationService/CalculateAttribute")
	}
	return &catalogv1.ScheduleCategoryExpiryResponse{
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Location: &commonv1.PostalAddress{
					Region: recalculateProductVariantsReply.GetProductVariant().GetAddress().GetRegion(),
				},
			}},
			CategoryId: req.GetCategoryId(),
			Manager:    req.GetCategory().GetManager(),
		},
		Result:  &commonv1.OperationResult{Status: lintProductsReply.GetResult().GetStatus()},
		Outcome: &catalogv1.ScheduleCategoryExpiryResponse_Outcome{Status: calculateAttributeReply.GetResult().GetStatus()},
	}, nil
}

// categoryLifecycleServerScheduleFraudSignalExpirySteps are the ways to run
// acme.fraud.v1.FraudSignalLifecycleService/ScheduleFraudSignalExpiry, by name.
var categoryLifecycleServerScheduleFraudSignalExpirySteps = map[string]func(*CategoryLifecycleServer, context.Context, *fraudv1.ScheduleFraudSignalExpiryRequest) (*fraudv1.ScheduleFraudSignalExpiryResponse, error){
	"default": (*CategoryLifecycleServer).scheduleFraudSignalExpiryStep,
	"audited": (*CategoryLifecycleServer).scheduleFraudSignalExpiryAuditedStep,
}

func (s *CategoryLifecycleServer) scheduleFraudSignalExpiryStep(ctx context.Context, request *fraudv1.ScheduleFraudSignalExpiryRequest) (*fraudv1.ScheduleFraudSignalExpiryResponse, error) {
	return s.fraudSignalLifecycleClient.ScheduleFraudSignalExpiry(ctx, request)
}

func (s *CategoryLifecycleServer) scheduleFraudSignalExpiryAuditedStep(ctx context.Context, request *fraudv1.ScheduleFraudSignalExpiryRequest) (*fraudv1.ScheduleFraudSignalExpiryResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.fraud.v1.FraudSignalLifecycleService/ScheduleFraudSignalExpiry")
	return s.fraudSignalLifecycleClient.ScheduleFraudSignalExpiry(ctx, request)
}

// createNotificationSender returns a function that sends one request to
// acme.notifications.v1.NotificationService/CreateNotification.
func (s *CategoryLifecycleServer) createNotificationSender(request *notificationsv1.CreateNotificationRequest) func(context.Context) (*notificationsv1.CreateNotificationResponse, error) {
	return func(ctx context.Context) (*notificationsv1.CreateNotificationResponse, error) {
		return s.notificationClient.CreateNotification(ctx, request)
	}
}

// categoryLifecycleServerRenewCategoryRetryStockMovementWorkflowStep carries the request of one step of RenewCategory
// and, once it ran, its reply.
type categoryLifecycleServerRenewCategoryRetryStockMovementWorkflowStep struct {
	request *inventoryv1.RetryStockMovementWorkflowRequest
	reply   *inventoryv1.RetryStockMovementWorkflowResponse
}

func (s *CategoryLifecycleServer) runRenewCategoryRetryStockMovementWorkflow(ctx context.Context, step *categoryLifecycleServerRenewCategoryRetryStockMovementWorkflowStep) error {
	reply, err := s.stockMovementWorkflowClient.RetryStockMovementWorkflow(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// cloneRateLimitSender returns a function that sends one request to
// acme.ratelimiting.v1.RateLimitService/CloneRateLimit.
func (s *CategoryLifecycleServer) cloneRateLimitSender(request *ratelimitingv1.CloneRateLimitRequest) func(context.Context) (*ratelimitingv1.CloneRateLimitResponse, error) {
	return func(ctx context.Context) (*ratelimitingv1.CloneRateLimitResponse, error) {
		return s.rateLimitClient.CloneRateLimit(ctx, request)
	}
}
