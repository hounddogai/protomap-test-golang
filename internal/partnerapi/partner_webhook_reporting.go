package partnerapi

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	webbffv1 "github.com/hounddogai/protomap-test-golang/gen/acme/webbff/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PartnerWebhookReportingServer implements `acme.partnerapi.v1.PartnerWebhookReportingService`.
// Builds and schedules reports about partner webhooks.
type PartnerWebhookReportingServer struct {
	partnerapiv1.UnsafePartnerWebhookReportingServiceServer
	store                               *Store
	log                                 *slog.Logger
	metricSeriesClient                  metricsv1.MetricSeriesServiceClient
	partnerIntegrationReportingClient   partnerapiv1.PartnerIntegrationReportingServiceClient
	sellerListingApprovalClient         sellersv1.SellerListingApprovalServiceClient
	conn                                *grpc.ClientConn
	loyaltyMemberWorkflowOnce           sync.Once
	loyaltyMemberWorkflowCached         loyaltyv1.LoyaltyMemberWorkflowServiceClient
	loyaltyMemberWorkflowClient         loyaltyv1.LoyaltyMemberWorkflowServiceClient
	loyaltyMemberWorkflowClientFallback loyaltyv1.LoyaltyMemberWorkflowServiceClient
	partnerWebhookApprovalClient        partnerapiv1.PartnerWebhookApprovalServiceClient
	meteredPartnerWebhookValidation     meteredPartnerWebhookValidation
	disputeSyncClient                   paymentsv1.DisputeSyncServiceClient
	partnerOrderReportingClient         partnerapiv1.PartnerOrderReportingServiceClient
	partnerIntegrationReportingPort     partnerIntegrationReportingPort
	orderNoteSyncClient                 ordersv1.OrderNoteSyncServiceClient
	partnerOrderApprovalClient          partnerapiv1.PartnerOrderApprovalServiceClient
	sellerCalculationClient             sellersv1.SellerCalculationServiceClient
	partnerWebhookValidationClient      partnerapiv1.PartnerWebhookValidationServiceClient
	sellerLifecycleClient               sellersv1.SellerLifecycleServiceClient
}

// NewPartnerWebhookReportingServer wires a PartnerWebhookReportingServer.
func NewPartnerWebhookReportingServer(deps platform.Deps) *PartnerWebhookReportingServer {
	return &PartnerWebhookReportingServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.partnerapi.v1.PartnerWebhookReportingService"),
		metricSeriesClient:                  metricsv1.NewMetricSeriesServiceClient(deps.Conn),
		partnerIntegrationReportingClient:   partnerapiv1.NewPartnerIntegrationReportingServiceClient(deps.Conn),
		sellerListingApprovalClient:         sellersv1.NewSellerListingApprovalServiceClient(deps.Conn),
		conn:                                deps.Conn,
		loyaltyMemberWorkflowClient:         loyaltyv1.NewLoyaltyMemberWorkflowServiceClient(deps.Conn),
		loyaltyMemberWorkflowClientFallback: loyaltyv1.NewLoyaltyMemberWorkflowServiceClient(deps.Conn),
		partnerWebhookApprovalClient:        partnerapiv1.NewPartnerWebhookApprovalServiceClient(deps.Conn),
		meteredPartnerWebhookValidation:     meteredPartnerWebhookValidation{PartnerWebhookValidationServiceClient: partnerapiv1.NewPartnerWebhookValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		disputeSyncClient:                   paymentsv1.NewDisputeSyncServiceClient(deps.Conn),
		partnerOrderReportingClient:         partnerapiv1.NewPartnerOrderReportingServiceClient(deps.Conn),
		partnerIntegrationReportingPort:     newPartnerIntegrationReportingPort(deps),
		orderNoteSyncClient:                 ordersv1.NewOrderNoteSyncServiceClient(deps.Conn),
		partnerOrderApprovalClient:          partnerapiv1.NewPartnerOrderApprovalServiceClient(deps.Conn),
		sellerCalculationClient:             sellersv1.NewSellerCalculationServiceClient(deps.Conn),
		partnerWebhookValidationClient:      partnerapiv1.NewPartnerWebhookValidationServiceClient(deps.Conn),
		sellerLifecycleClient:               sellersv1.NewSellerLifecycleServiceClient(deps.Conn),
	}
}

// GeneratePartnerWebhookReport generates a partner webhook.
func (s *PartnerWebhookReportingServer) GeneratePartnerWebhookReport(ctx context.Context, req *partnerapiv1.GeneratePartnerWebhookReportRequest) (*partnerapiv1.GeneratePartnerWebhookReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_webhook", req.GetPartnerWebhookId(), "generate_partner_webhook_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate partner webhook report", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "partner_webhook_id", req.GetPartnerWebhookId())
	updateMetricSeriesRequest := &metricsv1.UpdateMetricSeriesRequest{
		Context: req.GetContext(),
		MetricSeries: &metricsv1.MetricSeries{
			Owner: &identityv1.UserRef{
				UserId: req.GetContext().GetActor().GetUserId(),
				Email:  req.GetContext().GetActor().GetEmail(),
			},
			Components: map[string]*metricsv1.MetricSeriesComponent{"primary": &metricsv1.MetricSeriesComponent{ComponentId: tenancy.Value(ctx, "partner_webhook_id")}},
		},
	}
	updateMetricSeriesReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*metricsv1.UpdateMetricSeriesResponse, error) {
		return s.metricSeriesClient.UpdateMetricSeries(ctx, updateMetricSeriesRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesService/UpdateMetricSeries")
	}
	cloned := proto.Clone(req.GetDetails()).(*partnerapiv1.GeneratePartnerWebhookReportRequest_Details)
	schedulePartnerIntegrationReportRequest := &partnerapiv1.SchedulePartnerIntegrationReportRequest{}
	schedulePartnerIntegrationReportRequest.Context = req.GetContext()
	schedulePartnerIntegrationReportRequest.Details = &partnerapiv1.SchedulePartnerIntegrationReportRequest_Details{
		Filters: req.GetDetails().GetFilters(),
		Format:  cloned.GetFormat(),
	}
	schedulePartnerIntegrationReportReply, err := s.partnerIntegrationReportingClient.SchedulePartnerIntegrationReport(ctx, schedulePartnerIntegrationReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationReportingService/SchedulePartnerIntegrationReport")
	}
	commentSellerListingApprovalReply, err := s.sellerListingApprovalClient.CommentSellerListingApproval(ctx, &sellersv1.CommentSellerListingApprovalRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: updateMetricSeriesReply.GetMetricSeries().GetAudit().GetUpdatedBy(),
			},
			Locator: &sellersv1.SellerListing_Address{Address: &commonv1.PostalAddress{
				PostalCode: updateMetricSeriesReply.GetMetricSeries().GetAddress().GetPostalCode(),
			}},
			Components: map[string]*sellersv1.SellerListingComponent{"primary": &sellersv1.SellerListingComponent{
				ComponentId: updateMetricSeriesReply.GetMetricSeries().GetComponents()["primary"].GetComponentId(),
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingApprovalService/CommentSellerListingApproval")
	}
	pauseLoyaltyMemberWorkflowRequest := &loyaltyv1.PauseLoyaltyMemberWorkflowRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Audit: &commonv1.AuditInfo{
				CreatedBy: updateMetricSeriesReply.GetMetricSeries().GetAudit().GetCreatedBy(),
				Etag:      commentSellerListingApprovalReply.GetSellerListing().GetAudit().GetEtag(),
			},
			Holder: &loyaltyv1.RewardRef{
				DisplayName: commentSellerListingApprovalReply.GetSellerListing().GetOwner().GetDisplayName(),
			},
		},
		Details: &loyaltyv1.PauseLoyaltyMemberWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: updateMetricSeriesReply.GetMetricSeries().GetOwner().GetEmail(),
			},
		},
	}
	pauseLoyaltyMemberWorkflowReply, err := s.loyaltyMemberWorkflowLazy().PauseLoyaltyMemberWorkflow(ctx, pauseLoyaltyMemberWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberWorkflowService/PauseLoyaltyMemberWorkflow")
	}
	return &partnerapiv1.GeneratePartnerWebhookReportResponse{
		Result: &commonv1.OperationResult{
			Message:      updateMetricSeriesReply.GetResult().GetMessage(),
			Details:      updateMetricSeriesReply.GetResult().GetDetails(),
			Status:       commentSellerListingApprovalReply.GetResult().GetStatus(),
			ReferenceIds: pauseLoyaltyMemberWorkflowReply.GetResult().GetReferenceIds(),
		},
		Period:   schedulePartnerIntegrationReportReply.GetPeriod(),
		ReportId: schedulePartnerIntegrationReportReply.GetReportId(),
	}, nil
}

// FetchPartnerWebhookReport returns a partner webhook.
func (s *PartnerWebhookReportingServer) FetchPartnerWebhookReport(ctx context.Context, req *partnerapiv1.FetchPartnerWebhookReportRequest) (*partnerapiv1.FetchPartnerWebhookReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_webhook", req.GetPartnerWebhookId(), "fetch_partner_webhook_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch partner webhook report", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *loyaltyv1.ResumeLoyaltyMemberWorkflowRequest, ...grpc.CallOption) (*loyaltyv1.ResumeLoyaltyMemberWorkflowResponse, error){
		"primary":  s.loyaltyMemberWorkflowClient.ResumeLoyaltyMemberWorkflow,
		"fallback": s.loyaltyMemberWorkflowClientFallback.ResumeLoyaltyMemberWorkflow,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	resumeLoyaltyMemberWorkflowRequest := &loyaltyv1.ResumeLoyaltyMemberWorkflowRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Contact: &commonv1.ContactInfo{Email: cloned.GetActor().GetEmail()},
			Funding: &loyaltyv1.LoyaltyMember_ExternalAccountId{ExternalAccountId: req.GetPartnerWebhookId()},
		},
	}
	resumeLoyaltyMemberWorkflowReply, err := route(ctx, resumeLoyaltyMemberWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberWorkflowService/ResumeLoyaltyMemberWorkflow")
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	rejectPartnerWebhookRequest := &partnerapiv1.RejectPartnerWebhookRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.RejectPartnerWebhookRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: resumeLoyaltyMemberWorkflowReply.GetLoyaltyMember().GetHolder().GetDisplayName(),
			},
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				CurrencyCode: resumeLoyaltyMemberWorkflowReply.GetLoyaltyMember().GetCurrencyCode(),
			},
			Detail: packed,
		},
	}
	type rejectPartnerWebhookResult struct {
		reply *partnerapiv1.RejectPartnerWebhookResponse
		err   error
	}
	rejectPartnerWebhookResults := make(chan rejectPartnerWebhookResult, 1)
	go func() {
		reply, err := s.partnerWebhookApprovalClient.RejectPartnerWebhook(ctx, rejectPartnerWebhookRequest)
		rejectPartnerWebhookResults <- rejectPartnerWebhookResult{reply, err}
	}()
	var rejectPartnerWebhookReply *partnerapiv1.RejectPartnerWebhookResponse
	select {
	case result := <-rejectPartnerWebhookResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.partnerapi.v1.PartnerWebhookApprovalService/RejectPartnerWebhook")
		}
		rejectPartnerWebhookReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &partnerapiv1.FetchPartnerWebhookReportResponse{
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				CurrencyCode: resumeLoyaltyMemberWorkflowReply.GetLoyaltyMember().GetCurrencyCode(),
				DisplayName:  rejectPartnerWebhookReply.GetPartnerWebhook().GetSubject().GetDisplayName(),
			},
			CorrelationId:    rejectPartnerWebhookReply.GetPartnerWebhook().GetCorrelationId(),
			PartnerWebhookId: req.GetPartnerWebhookId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SchedulePartnerWebhookReport schedules a partner webhook.
func (s *PartnerWebhookReportingServer) SchedulePartnerWebhookReport(ctx context.Context, req *partnerapiv1.SchedulePartnerWebhookReportRequest) (*partnerapiv1.SchedulePartnerWebhookReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SchedulePartnerWebhookReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_webhook", req.GetPartnerWebhookId(), "schedule_partner_webhook_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule partner webhook report", "tenant", req.GetContext().GetTenantId())
	filters := make(map[string]string, len(req.GetDetails().GetFilters()))
	for key, value := range req.GetDetails().GetFilters() {
		filters[key] = value
	}
	createPageCompositionRequest := &webbffv1.CreatePageCompositionRequest{
		Context: req.GetContext(),
		PageComposition: &webbffv1.PageComposition{
			Lines: []*webbffv1.PageCompositionLine{&webbffv1.PageCompositionLine{Attributes: filters}},
		},
	}
	createPageCompositionReply, err := createPageCompositionCall(ctx, createPageCompositionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.webbff.v1.PageCompositionService/CreatePageComposition")
	}
	getPartnerWebhookRulesRequest := &partnerapiv1.GetPartnerWebhookRulesRequest{
		Context:          req.GetContext(),
		PartnerWebhookId: req.GetPartnerWebhookId(),
	}
	getPartnerWebhookRulesReply, err := s.meteredPartnerWebhookValidation.GetPartnerWebhookRules(ctx, getPartnerWebhookRulesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/GetPartnerWebhookRules")
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	reconcileDisputesRequest := &paymentsv1.ReconcileDisputesRequest{
		Context: req.GetContext(),
		Dispute: &paymentsv1.Dispute{
			Origin: &paymentsv1.Dispute_Customer{Customer: &paymentsv1.PaymentRef{
				DisplayName: getPartnerWebhookRulesReply.GetPartnerWebhook().GetSubject().GetDisplayName(),
			}},
		},
		Details: &paymentsv1.ReconcileDisputesRequest_Details{Payload: snapshot},
	}
	reconcileDisputesReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*paymentsv1.ReconcileDisputesResponse, error) {
		return s.disputeSyncClient.ReconcileDisputes(ctx, reconcileDisputesRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeSyncService/ReconcileDisputes")
	}
	schedulePartnerOrderReportRequest := newSchedulePartnerOrderReportRequest(withSchedulePartnerOrderReportContext(req.GetContext()), withSchedulePartnerOrderReportDetails(&partnerapiv1.SchedulePartnerOrderReportRequest_Details{
		RecipientEmails: req.GetDetails().GetRecipientEmails(),
		Format:          req.GetDetails().GetFormat(),
	}))
	schedulePartnerOrderReportReply, err := s.partnerOrderReportingClient.SchedulePartnerOrderReport(ctx, schedulePartnerOrderReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderReportingService/SchedulePartnerOrderReport")
	}
	_ = schedulePartnerOrderReportReply
	return &partnerapiv1.SchedulePartnerWebhookReportResponse{
		Result: &commonv1.OperationResult{
			Details:       createPageCompositionReply.GetResult().GetDetails(),
			CorrelationId: getPartnerWebhookRulesReply.GetPartnerWebhook().GetCorrelationId(),
			Message:       reconcileDisputesReply.GetOutcome().GetMessage(),
			ReferenceIds:  reconcileDisputesReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// StreamPartnerWebhookMetrics streams the changes of partner webhooks as they happen.
func (s *PartnerWebhookReportingServer) StreamPartnerWebhookMetrics(req *partnerapiv1.StreamPartnerWebhookMetricsRequest, stream grpc.ServerStreamingServer[partnerapiv1.StreamPartnerWebhookMetricsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		return err
	}
	s.log.InfoContext(ctx, "stream partner webhook metrics", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	generatePartnerIntegrationReportRequest := &partnerapiv1.GeneratePartnerIntegrationReportRequest{
		Context:     req.GetContext(),
		Details:     &partnerapiv1.GeneratePartnerIntegrationReportRequest_Details{Filters: labels},
		Annotations: req.GetContext().GetBaggage(),
	}
	generatePartnerIntegrationReportReply, err := s.partnerIntegrationReportingPort.GeneratePartnerIntegrationReport(ctx, generatePartnerIntegrationReportRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationReportingService/GeneratePartnerIntegrationReport")
	}
	listOrderNoteConflictsRequest := &ordersv1.ListOrderNoteConflictsRequest{}
	listOrderNoteConflictsRequest.Context = req.GetContext()
	listOrderNoteConflictsRequest.Filter = &ordersv1.ListOrderNoteConflictsRequest_Filter{
		Query:        req.GetFilter().GetQuery(),
		CreatedAfter: req.GetFilter().GetCreatedAfter(),
	}
	listOrderNoteConflictsReply, err := s.orderNoteSyncClient.ListOrderNoteConflicts(ctx, listOrderNoteConflictsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.orders.v1.OrderNoteSyncService/ListOrderNoteConflicts")
	}
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	escalatePartnerOrderRequest := &partnerapiv1.EscalatePartnerOrderRequest{
		Context: req.GetContext(),
		PartnerOrder: &partnerapiv1.PartnerOrder{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			Counterparty:   &sellersv1.SellerRef{Email: tenancy.Value(ctx, "email")},
		},
		Details: &partnerapiv1.EscalatePartnerOrderRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetSubscriber().GetEmail()},
		},
	}
	type escalatePartnerOrderResult struct {
		reply *partnerapiv1.EscalatePartnerOrderResponse
		err   error
	}
	escalatePartnerOrderResults := make(chan escalatePartnerOrderResult, 1)
	go func() {
		reply, err := s.partnerOrderApprovalClient.EscalatePartnerOrder(ctx, escalatePartnerOrderRequest)
		escalatePartnerOrderResults <- escalatePartnerOrderResult{reply, err}
	}()
	var escalatePartnerOrderReply *partnerapiv1.EscalatePartnerOrderResponse
	select {
	case result := <-escalatePartnerOrderResults:
		if result.err != nil {
			return grpcx.Wrap(result.err, "acme.partnerapi.v1.PartnerOrderApprovalService/EscalatePartnerOrder")
		}
		escalatePartnerOrderReply = result.reply
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
	calculateSellerRequest := &sellersv1.CalculateSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			PrimaryAddress: &commonv1.PostalAddress{
				Lines: escalatePartnerOrderReply.GetPartnerOrder().GetBillingAddress().GetLines(),
			},
			RelationshipManager: &identityv1.UserRef{
				Email: escalatePartnerOrderReply.GetPartnerOrder().GetCounterparty().GetEmail(),
			},
		},
		Details: &sellersv1.CalculateSellerRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region:     escalatePartnerOrderReply.GetPartnerOrder().GetBillingAddress().GetRegion(),
				PostalCode: escalatePartnerOrderReply.GetPartnerOrder().GetBillingAddress().GetPostalCode(),
			},
		},
	}
	type calculateSellerResult struct {
		reply *sellersv1.CalculateSellerResponse
		err   error
	}
	calculateSellerResults := make(chan calculateSellerResult, 1)
	go func() {
		reply, err := s.sellerCalculationClient.CalculateSeller(ctx, calculateSellerRequest)
		calculateSellerResults <- calculateSellerResult{reply, err}
	}()
	var calculateSellerReply *sellersv1.CalculateSellerResponse
	select {
	case result2 := <-calculateSellerResults:
		if result2.err != nil {
			return grpcx.Wrap(result2.err, "acme.sellers.v1.SellerCalculationService/CalculateSeller")
		}
		calculateSellerReply = result2.reply
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
	if err := stream.Send(&partnerapiv1.StreamPartnerWebhookMetricsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: generatePartnerIntegrationReportReply.GetResult().GetCorrelationId(),
			Status:        generatePartnerIntegrationReportReply.GetResult().GetStatus(),
			Details:       listOrderNoteConflictsReply.GetResult().GetDetails(),
			Message:       calculateSellerReply.GetResult().GetMessage(),
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			CorrelationId: listOrderNoteConflictsReply.GetResult().GetCorrelationId(),
			Subject: &partnerapiv1.PartnerIntegrationRef{
				DisplayName: escalatePartnerOrderReply.GetPartnerOrder().GetCounterparty().GetDisplayName(),
			},
		},
	}); err != nil {
		return err
	}
	return nil
}

// ListPartnerWebhookReports lists partner webhooks a page at a time.
func (s *PartnerWebhookReportingServer) ListPartnerWebhookReports(ctx context.Context, req *partnerapiv1.ListPartnerWebhookReportsRequest) (*partnerapiv1.ListPartnerWebhookReportsResponse, error) {
	s.log.InfoContext(ctx, "list partner webhook reports", "tenant", req.GetContext().GetTenantId())
	return &partnerapiv1.ListPartnerWebhookReportsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SharePartnerWebhookReport shares a partner webhook.
func (s *PartnerWebhookReportingServer) SharePartnerWebhookReport(ctx context.Context, req *partnerapiv1.SharePartnerWebhookReportRequest) (*partnerapiv1.SharePartnerWebhookReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SharePartnerWebhookReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_webhook", req.GetPartnerWebhookId(), "share_partner_webhook_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share partner webhook report", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	sharePartnerOrderReportReply, err := s.partnerOrderReportingClient.SharePartnerOrderReport(ctx, &partnerapiv1.SharePartnerOrderReportRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.SharePartnerOrderReportRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetDetails().GetRecipient().GetEmail()},
			Payload:   packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderReportingService/SharePartnerOrderReport")
	}
	countPartnerIntegrationTotalsRequest := buildCountPartnerIntegrationTotalsRequestForSharePartnerWebhookReport(req, sharePartnerOrderReportReply)
	countPartnerIntegrationTotalsReply, err := s.partnerIntegrationReportingClient.CountPartnerIntegrationTotals(ctx, countPartnerIntegrationTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationReportingService/CountPartnerIntegrationTotals")
	}
	packed2, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	validatePartnerWebhookRequest := &partnerapiv1.ValidatePartnerWebhookRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.ValidatePartnerWebhookRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:  req.GetDetails().GetRecipient().GetName(),
				Email: sharePartnerOrderReportReply.GetPartnerOrder().GetCounterparty().GetEmail(),
			},
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				DisplayName: sharePartnerOrderReportReply.GetPartnerOrder().GetCounterparty().GetDisplayName(),
			},
			CorrelationId: sharePartnerOrderReportReply.GetResult().GetCorrelationId(),
			Detail:        packed2,
		},
		PartnerWebhookId: envelope.GetDisplayName(),
	}
	validatePartnerWebhookReply := new(partnerapiv1.ValidatePartnerWebhookResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", partnerapiPartnerWebhookValidationServiceName, "ValidatePartnerWebhook"), validatePartnerWebhookRequest, validatePartnerWebhookReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/ValidatePartnerWebhook")
	}
	packed3, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	lintPartnerWebhooksReply, err := s.partnerWebhookValidationClient.LintPartnerWebhooks(ctx, &partnerapiv1.LintPartnerWebhooksRequest{
		Context: req.GetContext(),
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				DisplayName:          validatePartnerWebhookReply.GetPartnerWebhook().GetSubject().GetDisplayName(),
				PartnerIntegrationId: validatePartnerWebhookReply.GetPartnerWebhook().GetSubject().GetPartnerIntegrationId(),
			},
			Detail: packed3,
		},
		Details: &partnerapiv1.LintPartnerWebhooksRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: sharePartnerOrderReportReply.GetPartnerOrder().GetCounterparty().GetEmail(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/LintPartnerWebhooks")
	}
	return &partnerapiv1.SharePartnerWebhookReportResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: sharePartnerOrderReportReply.GetResult().GetReferenceIds(),
			Message:      countPartnerIntegrationTotalsReply.GetResult().GetMessage(),
			Status:       lintPartnerWebhooksReply.GetResult().GetStatus(),
		},
		Outcome: &partnerapiv1.SharePartnerWebhookReportResponse_Outcome{
			Message: validatePartnerWebhookReply.GetOutcome().GetMessage(),
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{PartnerWebhookId: req.GetPartnerWebhookId()},
	}, nil
}

// CountPartnerWebhookTotals counts partner webhooks by status.
func (s *PartnerWebhookReportingServer) CountPartnerWebhookTotals(ctx context.Context, req *partnerapiv1.CountPartnerWebhookTotalsRequest) (*partnerapiv1.CountPartnerWebhookTotalsResponse, error) {
	s.log.InfoContext(ctx, "count partner webhook totals", "tenant", req.GetContext().GetTenantId())
	scheduleSellerExpiryRequest := &sellersv1.ScheduleSellerExpiryRequest{}
	scheduleSellerExpiryRequest.Context = req.GetContext()
	scheduleSellerExpiryRequest.Seller = &sellersv1.Seller{
		PrimaryAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
	}
	scheduleSellerExpiryRequest.Details = &sellersv1.ScheduleSellerExpiryRequest_Details{
		Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
	}
	scheduleSellerExpiryReply, err := s.sellerLifecycleClient.ScheduleSellerExpiry(ctx, scheduleSellerExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerLifecycleService/ScheduleSellerExpiry")
	}
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	escalatePartnerWebhookRequest := newEscalatePartnerWebhookRequest(withEscalatePartnerWebhookContext(req.GetContext()), withEscalatePartnerWebhookDetails(&partnerapiv1.EscalatePartnerWebhookRequest_Details{
		Approver: &identityv1.UserRef{
			UserId:      scheduleSellerExpiryReply.GetSeller().GetRelationshipManager().GetUserId(),
			Email:       scheduleSellerExpiryReply.GetSeller().GetContact().GetEmail(),
			DisplayName: scheduleSellerExpiryReply.GetSeller().GetRelationshipManager().GetDisplayName(),
		},
	}), withEscalatePartnerWebhookPartnerWebhook(&partnerapiv1.PartnerWebhook{
		Attributes: scheduleSellerExpiryReply.GetSeller().GetAttributes(),
		Detail:     packed,
	}))
	escalatePartnerWebhookReply, err := s.partnerWebhookApprovalClient.EscalatePartnerWebhook(ctx, escalatePartnerWebhookRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookApprovalService/EscalatePartnerWebhook")
	}
	return &partnerapiv1.CountPartnerWebhookTotalsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  scheduleSellerExpiryReply.GetResult().GetReferenceIds(),
			CorrelationId: escalatePartnerWebhookReply.GetPartnerWebhook().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// loyaltyMemberWorkflowLazy dials acme.loyalty.v1.LoyaltyMemberWorkflowService the first time a handler needs it.
func (s *PartnerWebhookReportingServer) loyaltyMemberWorkflowLazy() loyaltyv1.LoyaltyMemberWorkflowServiceClient {
	s.loyaltyMemberWorkflowOnce.Do(func() { s.loyaltyMemberWorkflowCached = loyaltyv1.NewLoyaltyMemberWorkflowServiceClient(s.conn) })
	return s.loyaltyMemberWorkflowCached
}
