package orders

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	prescriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/prescriptions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	telehealthv1 "github.com/hounddogai/protomap-test-golang/gen/acme/telehealth/v1"
	timekeepingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/timekeeping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// OrderNoteReportingServer implements `acme.orders.v1.OrderNoteReportingService`.
// Builds and schedules reports about order notes.
type OrderNoteReportingServer struct {
	ordersv1.UnimplementedOrderNoteReportingServiceServer
	store                      *Store
	log                        *slog.Logger
	orderLifecycleClient       ordersv1.OrderLifecycleServiceClient
	orderValidationClient      ordersv1.OrderValidationServiceClient
	featureFlagQueryClient     featureflagsv1.FeatureFlagQueryServiceClient
	videoVisitClient           telehealthv1.VideoVisitServiceClient
	backorderCalculationClient ordersv1.BackorderCalculationServiceClient
	conn                       *grpc.ClientConn
	searchIndexClient          searchv1.SearchIndexServiceClient
	validateOrderNoteImportFn  func(context.Context, *ordersv1.ValidateOrderNoteImportRequest, ...grpc.CallOption) (*ordersv1.ValidateOrderNoteImportResponse, error)
	timesheetQueryClient       timekeepingv1.TimesheetQueryServiceClient
	bus                        *eventbus.Bus
	customerQueryOnce          sync.Once
	customerQueryCached        customersv1.CustomerQueryServiceClient
}

// NewOrderNoteReportingServer wires an OrderNoteReportingServer.
func NewOrderNoteReportingServer(deps platform.Deps) *OrderNoteReportingServer {
	orderNoteImport := ordersv1.NewOrderNoteImportServiceClient(deps.Conn)
	return &OrderNoteReportingServer{
		store:                      NewStore(deps.DB),
		log:                        deps.Logger.With("service", "acme.orders.v1.OrderNoteReportingService"),
		orderLifecycleClient:       ordersv1.NewOrderLifecycleServiceClient(deps.Conn),
		orderValidationClient:      ordersv1.NewOrderValidationServiceClient(deps.Conn),
		featureFlagQueryClient:     featureflagsv1.NewFeatureFlagQueryServiceClient(deps.Conn),
		videoVisitClient:           telehealthv1.NewVideoVisitServiceClient(deps.Conn),
		backorderCalculationClient: ordersv1.NewBackorderCalculationServiceClient(deps.Conn),
		conn:                       deps.Conn,
		searchIndexClient:          searchv1.NewSearchIndexServiceClient(deps.Conn),
		validateOrderNoteImportFn:  orderNoteImport.ValidateOrderNoteImport,
		timesheetQueryClient:       timekeepingv1.NewTimesheetQueryServiceClient(deps.Conn),
		bus:                        deps.Bus,
	}
}

// GenerateOrderNoteReport generates an order note.
func (s *OrderNoteReportingServer) GenerateOrderNoteReport(ctx context.Context, req *ordersv1.GenerateOrderNoteReportRequest) (*ordersv1.GenerateOrderNoteReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GenerateOrderNoteReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_note", req.GetOrderNoteId(), "generate_order_note_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate order note report", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	scheduleOrderExpiryReply, err := s.orderLifecycleClient.ScheduleOrderExpiry(ctx, &ordersv1.ScheduleOrderExpiryRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: annotations,
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderLifecycleService/ScheduleOrderExpiry")
	}
	explainOrderDecisionRequest := newExplainOrderDecisionRequest(withExplainOrderDecisionContext(req.GetContext()), withExplainOrderDecisionOrder(&ordersv1.Order{
		BillingAddress: &commonv1.PostalAddress{
			Region: scheduleOrderExpiryReply.GetOrder().GetBillingAddress().GetRegion(),
		},
		Audit: &commonv1.AuditInfo{
			CreatedBy: scheduleOrderExpiryReply.GetOrder().GetAudit().GetCreatedBy(),
		},
		Settlement: &ordersv1.Order_Card{Card: &ordersv1.OrderCardSettlement{
			HolderName: scheduleOrderExpiryReply.GetOrder().GetCard().GetHolderName(),
		}},
	}))
	explainOrderDecisionReply, err := s.orderValidationClient.ExplainOrderDecision(ctx, explainOrderDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderValidationService/ExplainOrderDecision")
	}
	_ = explainOrderDecisionReply
	return &ordersv1.GenerateOrderNoteReportResponse{
		Result: &commonv1.OperationResult{Status: scheduleOrderExpiryReply.GetResult().GetStatus()},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchOrderNoteReport returns an order note.
func (s *OrderNoteReportingServer) FetchOrderNoteReport(ctx context.Context, req *ordersv1.FetchOrderNoteReportRequest) (*ordersv1.FetchOrderNoteReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_note", req.GetOrderNoteId(), "fetch_order_note_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch order note report", "tenant", req.GetContext().GetTenantId())
	return &ordersv1.FetchOrderNoteReportResponse{
		OrderNote: &ordersv1.OrderNote{OrderNoteId: req.GetOrderNoteId()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScheduleOrderNoteReport schedules an order note.
func (s *OrderNoteReportingServer) ScheduleOrderNoteReport(ctx context.Context, req *ordersv1.ScheduleOrderNoteReportRequest) (*ordersv1.ScheduleOrderNoteReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_note", req.GetOrderNoteId(), "schedule_order_note_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule order note report", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	countFeatureFlagsRequest := &featureflagsv1.CountFeatureFlagsRequest{
		Context: req.GetContext(),
		Filter: &featureflagsv1.CountFeatureFlagsRequest_Filter{
			OwnerId: req.GetOrderNoteId(),
			Labels:  annotations,
		},
	}
	countFeatureFlagsReply, err := grpcx.Call(ctx, s.featureFlagQueryClient.CountFeatureFlags, countFeatureFlagsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/CountFeatureFlags")
	}
	ctx = tenancy.With(ctx, "order_note_id", req.GetOrderNoteId())
	searchVideoVisitsRequest := &telehealthv1.SearchVideoVisitsRequest{
		Context: req.GetContext(),
		Filter: &telehealthv1.SearchVideoVisitsRequest_Filter{
			Labels:       req.GetDetails().GetFilters(),
			OwnerId:      tenancy.Value(ctx, "order_note_id"),
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	}
	searchVideoVisitsReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*telehealthv1.SearchVideoVisitsResponse, error) {
		return s.videoVisitClient.SearchVideoVisits(ctx, searchVideoVisitsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.telehealth.v1.VideoVisitService/SearchVideoVisits")
	}
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	quoteBackorderRequest := &ordersv1.QuoteBackorderRequest{
		Context: req.GetContext(),
		Backorder: &ordersv1.Backorder{
			Assignee: &identityv1.UserRef{
				Email:  cloned.GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
		Annotations: req.GetAnnotations(),
	}
	quoteBackorderReply, err := ordersv1.BackorderCalculationServiceClient.QuoteBackorder(s.backorderCalculationClient, ctx, quoteBackorderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderCalculationService/QuoteBackorder")
	}
	filters := make(map[string]string, len(req.GetDetails().GetFilters()))
	for key2, value2 := range req.GetDetails().GetFilters() {
		filters[key2] = value2
	}
	listPrescriptionsRequest := &prescriptionsv1.ListPrescriptionsRequest{
		Context: req.GetContext(),
		Filter: &prescriptionsv1.ListPrescriptionsRequest_Filter{
			CreatedAfter: quoteBackorderReply.GetBackorder().GetSla().GetDueAt(),
			OwnerId:      countFeatureFlagsReply.GetResult().GetCorrelationId(),
			Labels:       filters,
		},
	}
	listPrescriptionsReply := new(prescriptionsv1.ListPrescriptionsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", prescriptionsPrescriptionServiceName, "ListPrescriptions"), listPrescriptionsRequest, listPrescriptionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.prescriptions.v1.PrescriptionService/ListPrescriptions")
	}
	_ = searchVideoVisitsReply
	return &ordersv1.ScheduleOrderNoteReportResponse{
		Result: &commonv1.OperationResult{
			Status:  countFeatureFlagsReply.GetResult().GetStatus(),
			Details: countFeatureFlagsReply.GetResult().GetDetails(),
			Message: quoteBackorderReply.GetResult().GetMessage(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// ListOrderNoteReports lists order notes a page at a time.
func (s *OrderNoteReportingServer) ListOrderNoteReports(ctx context.Context, req *ordersv1.ListOrderNoteReportsRequest) (*ordersv1.ListOrderNoteReportsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list order note reports", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "region", req.GetFilter().GetRegion())
	searchSearchIndexesRequest := &searchv1.SearchSearchIndexesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.SearchSearchIndexesRequest_Filter{
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			OwnerId:      req.GetFilter().GetOwnerId(),
			Region:       tenancy.Value(ctx, "region"),
		},
	}
	send := s.searchSearchIndexesSender(searchSearchIndexesRequest)
	searchSearchIndexesReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexService/SearchSearchIndexes")
	}
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	validateOrderNoteImportRequest := &ordersv1.ValidateOrderNoteImportRequest{
		Context: req.GetContext(),
		OrderNote: &ordersv1.OrderNote{
			CorrelationId: searchSearchIndexesReply.GetResult().GetCorrelationId(),
			Detail:        packed,
		},
		Details: &ordersv1.ValidateOrderNoteImportRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	validateOrderNoteImportReply, err := s.validateOrderNoteImportFn(ctx, validateOrderNoteImportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteImportService/ValidateOrderNoteImport")
	}
	return &ordersv1.ListOrderNoteReportsResponse{
		Result: &commonv1.OperationResult{
			Details:       searchSearchIndexesReply.GetResult().GetDetails(),
			CorrelationId: searchSearchIndexesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		OrderNotes: []*ordersv1.OrderNote{&ordersv1.OrderNote{
			Subject: validateOrderNoteImportReply.GetOrderNote().GetSubject(),
		}},
	}, nil
}

// CountOrderNoteTotals counts order notes by status.
func (s *OrderNoteReportingServer) CountOrderNoteTotals(ctx context.Context, req *ordersv1.CountOrderNoteTotalsRequest) (*ordersv1.CountOrderNoteTotalsResponse, error) {
	s.log.InfoContext(ctx, "count order note totals", "tenant", req.GetContext().GetTenantId())
	filterCache := cache.New[string, *ordersv1.CountOrderNoteTotalsRequest_Filter](1)
	filterCache.Put(req.GetContext().GetTraceId(), req.GetFilter())
	cachedFilter, _ := filterCache.Get(req.GetContext().GetTraceId())
	browseTimesheetsRequest := &timekeepingv1.BrowseTimesheetsRequest{}
	browseTimesheetsRequest.Context = req.GetContext()
	browseTimesheetsRequest.Filter = &timekeepingv1.BrowseTimesheetsRequest_Filter{
		Query:   req.GetFilter().GetQuery(),
		OwnerId: cachedFilter.GetOwnerId(),
	}
	browseTimesheetsReply, err := s.timesheetQueryClient.BrowseTimesheets(ctx, browseTimesheetsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.timekeeping.v1.TimesheetQueryService/BrowseTimesheets")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "orders.count_order_note_totals.count_order_amendments",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"page.page_token":   req.GetPage().GetPageToken(),
			"filter.owner_id":   req.GetFilter().GetOwnerId(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &ordersv1.CountOrderNoteTotalsResponse{
		Result: &commonv1.OperationResult{
			Details:       browseTimesheetsReply.GetResult().GetDetails(),
			CorrelationId: browseTimesheetsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// DeleteOrderNoteReport deletes an order note.
func (s *OrderNoteReportingServer) DeleteOrderNoteReport(ctx context.Context, req *ordersv1.DeleteOrderNoteReportRequest) (*ordersv1.DeleteOrderNoteReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_note", req.GetOrderNoteId(), "delete_order_note_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete order note report", "tenant", req.GetContext().GetTenantId())
	browseCustomersRequest := &customersv1.BrowseCustomersRequest{
		Context: req.GetContext(),
		Filter:  &customersv1.BrowseCustomersRequest_Filter{OwnerId: req.GetOrderNoteId()},
	}
	browseCustomersReply, err := s.customerQueryLazy().BrowseCustomers(ctx, browseCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/BrowseCustomers")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := orderNoteReportingServerScheduleOrderExpirySteps[mode]
	scheduleOrderExpiryRequest := &ordersv1.ScheduleOrderExpiryRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
	}
	scheduleOrderExpiryReply, err := run(s, ctx, scheduleOrderExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderLifecycleService/ScheduleOrderExpiry")
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	sendRoleTestNoticeRequest := &identityv1.SendRoleTestNoticeRequest{
		Context: req.GetContext(),
		Details: &identityv1.SendRoleTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email: scheduleOrderExpiryReply.GetOrder().GetCounterparty().GetEmail(),
			},
			Variables: scheduleOrderExpiryReply.GetResult().GetDetails(),
			Payload:   packed,
		},
		Reason:      req.GetReason(),
		Annotations: browseCustomersReply.GetResult().GetDetails(),
	}
	sendRoleTestNoticeReply, err := sendRoleTestNoticeCall(ctx, sendRoleTestNoticeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleNotificationService/SendRoleTestNotice")
	}
	packed2, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	lintOrderNotesRequest := &ordersv1.LintOrderNotesRequest{
		Context: req.GetContext(),
		Details: &ordersv1.LintOrderNotesRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: sendRoleTestNoticeReply.GetRole().GetOwner().GetEmail(),
			},
		},
		OrderNote: &ordersv1.OrderNote{
			CorrelationId: browseCustomersReply.GetResult().GetCorrelationId(),
			Detail:        packed2,
		},
	}
	lintOrderNotesReply, err := lintOrderNotesCall(ctx, lintOrderNotesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteValidationService/LintOrderNotes")
	}
	return &ordersv1.DeleteOrderNoteReportResponse{
		Result: &commonv1.OperationResult{
			Status:        browseCustomersReply.GetResult().GetStatus(),
			CorrelationId: browseCustomersReply.GetResult().GetCorrelationId(),
			Details:       sendRoleTestNoticeReply.GetResult().GetDetails(),
			Message:       lintOrderNotesReply.GetResult().GetMessage(),
		},
		EffectiveAt: lintOrderNotesReply.GetOutcome().GetEffectiveAt(),
	}, nil
}

// searchSearchIndexesSender returns a function that sends one request to
// acme.search.v1.SearchIndexService/SearchSearchIndexes.
func (s *OrderNoteReportingServer) searchSearchIndexesSender(request *searchv1.SearchSearchIndexesRequest) func(context.Context) (*searchv1.SearchSearchIndexesResponse, error) {
	return func(ctx context.Context) (*searchv1.SearchSearchIndexesResponse, error) {
		return s.searchIndexClient.SearchSearchIndexes(ctx, request)
	}
}

// customerQueryLazy dials acme.customers.v1.CustomerQueryService the first time a handler needs it.
func (s *OrderNoteReportingServer) customerQueryLazy() customersv1.CustomerQueryServiceClient {
	s.customerQueryOnce.Do(func() { s.customerQueryCached = customersv1.NewCustomerQueryServiceClient(s.conn) })
	return s.customerQueryCached
}

// orderNoteReportingServerScheduleOrderExpirySteps are the ways to run
// acme.orders.v1.OrderLifecycleService/ScheduleOrderExpiry, by name.
var orderNoteReportingServerScheduleOrderExpirySteps = map[string]func(*OrderNoteReportingServer, context.Context, *ordersv1.ScheduleOrderExpiryRequest) (*ordersv1.ScheduleOrderExpiryResponse, error){
	"default": (*OrderNoteReportingServer).scheduleOrderExpiryStep,
	"audited": (*OrderNoteReportingServer).scheduleOrderExpiryAuditedStep,
}

func (s *OrderNoteReportingServer) scheduleOrderExpiryStep(ctx context.Context, request *ordersv1.ScheduleOrderExpiryRequest) (*ordersv1.ScheduleOrderExpiryResponse, error) {
	return s.orderLifecycleClient.ScheduleOrderExpiry(ctx, request)
}

func (s *OrderNoteReportingServer) scheduleOrderExpiryAuditedStep(ctx context.Context, request *ordersv1.ScheduleOrderExpiryRequest) (*ordersv1.ScheduleOrderExpiryResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.orders.v1.OrderLifecycleService/ScheduleOrderExpiry")
	return s.orderLifecycleClient.ScheduleOrderExpiry(ctx, request)
}
