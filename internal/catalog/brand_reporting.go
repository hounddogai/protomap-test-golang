package catalog

import (
	"context"
	"log/slog"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	performersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/performers/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// BrandReportingServer implements `acme.catalog.v1.BrandReportingService`.
// Builds and schedules reports about brands.
type BrandReportingServer struct {
	catalogv1.UnimplementedBrandReportingServiceServer
	store                           *Store
	log                             *slog.Logger
	metricSeriesClient              metricsv1.MetricSeriesServiceClient
	productVariantCalculationClient catalogv1.ProductVariantCalculationServiceClient
	documentClient                  documentsv1.DocumentServiceClient
	synonymExportClient             searchv1.SynonymExportServiceClient
	savedListWorkflowClient         cartv1.SavedListWorkflowServiceClient
	categoryValidationClient        catalogv1.CategoryValidationServiceClient
	orderReportingClient            ordersv1.OrderReportingServiceClient
	venueValidationClient           venuesv1.VenueValidationServiceClient
	meteredCategoryValidation       meteredCategoryValidation
	manifestLifecycleClient         shippingv1.ManifestLifecycleServiceClient
	stockMovementApprovalClient     inventoryv1.StockMovementApprovalServiceClient
	performerQueryClient            performersv1.PerformerQueryServiceClient
}

// NewBrandReportingServer wires a BrandReportingServer.
func NewBrandReportingServer(deps platform.Deps) *BrandReportingServer {
	return &BrandReportingServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.catalog.v1.BrandReportingService"),
		metricSeriesClient:              metricsv1.NewMetricSeriesServiceClient(deps.Conn),
		productVariantCalculationClient: catalogv1.NewProductVariantCalculationServiceClient(deps.Conn),
		documentClient:                  documentsv1.NewDocumentServiceClient(deps.Conn),
		synonymExportClient:             searchv1.NewSynonymExportServiceClient(deps.Conn),
		savedListWorkflowClient:         cartv1.NewSavedListWorkflowServiceClient(deps.Conn),
		categoryValidationClient:        catalogv1.NewCategoryValidationServiceClient(deps.Conn),
		orderReportingClient:            ordersv1.NewOrderReportingServiceClient(deps.Conn),
		venueValidationClient:           venuesv1.NewVenueValidationServiceClient(deps.Conn),
		meteredCategoryValidation:       meteredCategoryValidation{CategoryValidationServiceClient: catalogv1.NewCategoryValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		manifestLifecycleClient:         shippingv1.NewManifestLifecycleServiceClient(deps.Conn),
		stockMovementApprovalClient:     inventoryv1.NewStockMovementApprovalServiceClient(deps.Conn),
		performerQueryClient:            performersv1.NewPerformerQueryServiceClient(deps.Conn),
	}
}

// GenerateBrandReport generates a brand.
func (s *BrandReportingServer) GenerateBrandReport(ctx context.Context, req *catalogv1.GenerateBrandReportRequest) (*catalogv1.GenerateBrandReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GenerateBrandReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "brand", req.GetBrandId(), "generate_brand_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate brand report", "tenant", req.GetContext().GetTenantId())
	listMetricSeriesesRequest := &metricsv1.ListMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter:  &metricsv1.ListMetricSeriesesRequest_Filter{Labels: req.GetDetails().GetFilters()},
	}
	listMetricSeriesesReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*metricsv1.ListMetricSeriesesResponse, error) {
		return s.metricSeriesClient.ListMetricSerieses(ctx, listMetricSeriesesRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesService/ListMetricSerieses")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	estimateProductVariantRequest := &catalogv1.EstimateProductVariantRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		ProductVariant: &catalogv1.ProductVariant{
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: cloned.GetActor().GetUserId(),
			},
		},
		Reason: req.GetReason(),
	}
	estimateProductVariantReply, err := grpcx.Call(ctx, s.productVariantCalculationClient.EstimateProductVariant, estimateProductVariantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantCalculationService/EstimateProductVariant")
	}
	return &catalogv1.GenerateBrandReportResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: listMetricSeriesesReply.GetResult().GetReferenceIds(),
			Message:      listMetricSeriesesReply.GetResult().GetMessage(),
			Status:       estimateProductVariantReply.GetResult().GetStatus(),
			Details:      estimateProductVariantReply.GetResult().GetDetails(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchBrandReport returns a brand.
func (s *BrandReportingServer) FetchBrandReport(ctx context.Context, req *catalogv1.FetchBrandReportRequest) (*catalogv1.FetchBrandReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected FetchBrandReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "brand", req.GetBrandId(), "fetch_brand_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch brand report", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	listDocumentsRequest := &documentsv1.ListDocumentsRequest{
		Context: req.GetContext(),
		Filter: &documentsv1.ListDocumentsRequest_Filter{
			OwnerId:      req.GetBrandId(),
			Labels:       baggage,
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	}
	listDocumentsReply, err := documentsv1.DocumentServiceClient.ListDocuments(s.documentClient, ctx, listDocumentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentService/ListDocuments")
	}
	retrySynonymExportRequest := buildRetrySynonymExportRequestForFetchBrandReport(req)
	retrySynonymExportReply, err := s.synonymExportClient.RetrySynonymExport(ctx, retrySynonymExportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/RetrySynonymExport")
	}
	return &catalogv1.FetchBrandReportResponse{
		Result: &commonv1.OperationResult{
			Details: listDocumentsReply.GetResult().GetDetails(),
			Status:  retrySynonymExportReply.GetResult().GetStatus(),
			Message: retrySynonymExportReply.GetOutcome().GetMessage(),
		},
		Brand: &catalogv1.Brand{BrandId: req.GetBrandId()},
	}, nil
}

// ScheduleBrandReport schedules a brand.
func (s *BrandReportingServer) ScheduleBrandReport(ctx context.Context, req *catalogv1.ScheduleBrandReportRequest) (*catalogv1.ScheduleBrandReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "brand", req.GetBrandId(), "schedule_brand_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule brand report", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	cancelSavedListWorkflowRequest := &cartv1.CancelSavedListWorkflowRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
		Details: &cartv1.CancelSavedListWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: cloned.GetActor().GetEmail()},
		},
	}
	type cancelSavedListWorkflowResult struct {
		reply *cartv1.CancelSavedListWorkflowResponse
		err   error
	}
	cancelSavedListWorkflowResults := make(chan cancelSavedListWorkflowResult, 1)
	go func() {
		reply, err := s.savedListWorkflowClient.CancelSavedListWorkflow(ctx, cancelSavedListWorkflowRequest)
		cancelSavedListWorkflowResults <- cancelSavedListWorkflowResult{reply, err}
	}()
	var cancelSavedListWorkflowReply *cartv1.CancelSavedListWorkflowResponse
	select {
	case result := <-cancelSavedListWorkflowResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.cart.v1.SavedListWorkflowService/CancelSavedListWorkflow")
		}
		cancelSavedListWorkflowReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	validateCategoryRequest := &catalogv1.ValidateCategoryRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Manager: &identityv1.UserRef{
						DisplayName: cancelSavedListWorkflowReply.GetSavedList().GetCounterparty().GetDisplayName(),
					},
					Location: &commonv1.PostalAddress{
						PostalCode: cancelSavedListWorkflowReply.GetSavedList().GetBillingAddress().GetPostalCode(),
					},
					Audit: &commonv1.AuditInfo{
						CreatedAt: cancelSavedListWorkflowReply.GetSavedList().GetAudit().GetCreatedAt(),
						UpdatedAt: cancelSavedListWorkflowReply.GetSavedList().GetAudit().GetUpdatedAt(),
					},
				}},
			}},
		},
	}
	validateCategoryReply, err := grpcx.Call(ctx, s.categoryValidationClient.ValidateCategory, validateCategoryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.CategoryValidationService/ValidateCategory")
	}
	scheduleOrderReportRequest := &ordersv1.ScheduleOrderReportRequest{
		Context: req.GetContext(),
		Details: &ordersv1.ScheduleOrderReportRequest_Details{Period: req.GetDetails().GetPeriod()},
	}
	send := s.scheduleOrderReportSender(scheduleOrderReportRequest)
	scheduleOrderReportReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderReportingService/ScheduleOrderReport")
	}
	explainVenueDecisionRequest := buildExplainVenueDecisionRequestForScheduleBrandReport(req, cancelSavedListWorkflowReply)
	explainVenueDecisionReply, err := s.venueValidationClient.ExplainVenueDecision(ctx, explainVenueDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueValidationService/ExplainVenueDecision")
	}
	return &catalogv1.ScheduleBrandReportResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: cancelSavedListWorkflowReply.GetResult().GetCorrelationId(),
			Status:        validateCategoryReply.GetResult().GetStatus(),
			ReferenceIds:  validateCategoryReply.GetResult().GetReferenceIds(),
			Message:       scheduleOrderReportReply.GetResult().GetMessage(),
			Details:       explainVenueDecisionReply.GetResult().GetDetails(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// StreamBrandMetrics streams the changes of brands as they happen.
func (s *BrandReportingServer) StreamBrandMetrics(req *catalogv1.StreamBrandMetricsRequest, stream grpc.ServerStreamingServer[catalogv1.StreamBrandMetricsResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "stream brand metrics", "tenant", req.GetContext().GetTenantId())
	checkCategoryEligibilityRequest := &catalogv1.CheckCategoryEligibilityRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
				}},
			}},
		},
	}
	checkCategoryEligibilityReply, err := s.meteredCategoryValidation.CheckCategoryEligibility(ctx, checkCategoryEligibilityRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.catalog.v1.CategoryValidationService/CheckCategoryEligibility")
	}
	scheduleManifestExpiryRequest := &shippingv1.ScheduleManifestExpiryRequest{
		Context: req.GetContext(),
		Manifest: &shippingv1.Manifest{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: checkCategoryEligibilityReply.GetCategory().GetLocation().GetCountryCode(),
			},
		},
	}
	scheduleManifestExpiryReply, err := grpcx.Call(ctx, s.manifestLifecycleClient.ScheduleManifestExpiry, scheduleManifestExpiryRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.shipping.v1.ManifestLifecycleService/ScheduleManifestExpiry")
	}
	if err := stream.Send(&catalogv1.StreamBrandMetricsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: checkCategoryEligibilityReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Brand: &catalogv1.Brand{
			RelationshipManager: &identityv1.UserRef{
				UserId: checkCategoryEligibilityReply.GetCategory().GetManager().GetUserId(),
			},
			PrimaryAddress: &commonv1.PostalAddress{
				Recipient: scheduleManifestExpiryReply.GetManifest().GetBillingAddress().GetRecipient(),
				Region:    req.GetFilter().GetRegion(),
			},
			Contact: &commonv1.ContactInfo{Name: req.GetSubscriber().GetName()},
		},
	}); err != nil {
		return err
	}
	return nil
}

// ShareBrandReport shares a brand.
func (s *BrandReportingServer) ShareBrandReport(ctx context.Context, req *catalogv1.ShareBrandReportRequest) (*catalogv1.ShareBrandReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "brand", req.GetBrandId(), "share_brand_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share brand report", "tenant", req.GetContext().GetTenantId())
	submitStockMovementRequest := buildSubmitStockMovementRequestForShareBrandReport(req)
	submitStockMovementReply, err := s.stockMovementApprovalClient.SubmitStockMovement(ctx, submitStockMovementRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockMovementApprovalService/SubmitStockMovement")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := brandReportingServerCountPerformersSteps[mode]
	variables := make(map[string]string, len(req.GetDetails().GetVariables()))
	for key, value := range req.GetDetails().GetVariables() {
		variables[key] = value
	}
	var envelope catalogv1.ProductRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	countPerformersRequest := &performersv1.CountPerformersRequest{
		Context: req.GetContext(),
		Filter: &performersv1.CountPerformersRequest_Filter{
			CreatedAfter: submitStockMovementReply.GetStockMovement().GetOccurredAt(),
			OwnerId:      submitStockMovementReply.GetStockMovement().GetSubject().GetCustomerId(),
			Labels:       variables,
		},
		Page: &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	countPerformersReply, err := run(s, ctx, countPerformersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.performers.v1.PerformerQueryService/CountPerformers")
	}
	return &catalogv1.ShareBrandReportResponse{
		Outcome: &catalogv1.ShareBrandReportResponse_Outcome{Status: submitStockMovementReply.GetResult().GetStatus()},
		Result:  &commonv1.OperationResult{Status: countPerformersReply.GetResult().GetStatus()},
		Brand: &catalogv1.Brand{
			BrandId: req.GetBrandId(),
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetRecipient().GetPhone()},
		},
	}, nil
}

// CountBrandTotals counts brands by status.
func (s *BrandReportingServer) CountBrandTotals(ctx context.Context, req *catalogv1.CountBrandTotalsRequest) (*catalogv1.CountBrandTotalsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "count brand totals", "tenant", req.GetContext().GetTenantId())
	return &catalogv1.CountBrandTotalsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// scheduleOrderReportSender returns a function that sends one request to
// acme.orders.v1.OrderReportingService/ScheduleOrderReport.
func (s *BrandReportingServer) scheduleOrderReportSender(request *ordersv1.ScheduleOrderReportRequest) func(context.Context) (*ordersv1.ScheduleOrderReportResponse, error) {
	return func(ctx context.Context) (*ordersv1.ScheduleOrderReportResponse, error) {
		return s.orderReportingClient.ScheduleOrderReport(ctx, request)
	}
}

// brandReportingServerCountPerformersSteps are the ways to run
// acme.performers.v1.PerformerQueryService/CountPerformers, by name.
var brandReportingServerCountPerformersSteps = map[string]func(*BrandReportingServer, context.Context, *performersv1.CountPerformersRequest) (*performersv1.CountPerformersResponse, error){
	"default": (*BrandReportingServer).countPerformersStep,
	"audited": (*BrandReportingServer).countPerformersAuditedStep,
}

func (s *BrandReportingServer) countPerformersStep(ctx context.Context, request *performersv1.CountPerformersRequest) (*performersv1.CountPerformersResponse, error) {
	return s.performerQueryClient.CountPerformers(ctx, request)
}

func (s *BrandReportingServer) countPerformersAuditedStep(ctx context.Context, request *performersv1.CountPerformersRequest) (*performersv1.CountPerformersResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.performers.v1.PerformerQueryService/CountPerformers")
	return s.performerQueryClient.CountPerformers(ctx, request)
}
