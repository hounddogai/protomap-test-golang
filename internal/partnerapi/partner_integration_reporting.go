package partnerapi

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PartnerIntegrationReportingServer implements `acme.partnerapi.v1.PartnerIntegrationReportingService`.
// Builds and schedules reports about partner integrations.
type PartnerIntegrationReportingServer struct {
	partnerapiv1.UnimplementedPartnerIntegrationReportingServiceServer
	store                           *Store
	log                             *slog.Logger
	conn                            *grpc.ClientConn
	summarizeMetricSeriesesFn       func(context.Context, *metricsv1.SummarizeMetricSeriesesRequest, ...grpc.CallOption) (*metricsv1.SummarizeMetricSeriesesResponse, error)
	partnerOrderApprovalClient      partnerapiv1.PartnerOrderApprovalServiceClient
	meteredSellerScorecardReporting meteredSellerScorecardReporting
	searchQueryHistoryPort          searchQueryHistoryPort
	bus                             *eventbus.Bus
	tierApprovalClient              loyaltyv1.TierApprovalServiceClient
	consentRecordLifecycleClient    customersv1.ConsentRecordLifecycleServiceClient
}

// NewPartnerIntegrationReportingServer wires a PartnerIntegrationReportingServer.
func NewPartnerIntegrationReportingServer(deps platform.Deps) *PartnerIntegrationReportingServer {
	metricSeriesQuery := metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn)
	return &PartnerIntegrationReportingServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.partnerapi.v1.PartnerIntegrationReportingService"),
		conn:                            deps.Conn,
		summarizeMetricSeriesesFn:       metricSeriesQuery.SummarizeMetricSerieses,
		partnerOrderApprovalClient:      partnerapiv1.NewPartnerOrderApprovalServiceClient(deps.Conn),
		meteredSellerScorecardReporting: meteredSellerScorecardReporting{SellerScorecardReportingServiceClient: sellersv1.NewSellerScorecardReportingServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		searchQueryHistoryPort:          newSearchQueryHistoryPort(deps),
		bus:                             deps.Bus,
		tierApprovalClient:              loyaltyv1.NewTierApprovalServiceClient(deps.Conn),
		consentRecordLifecycleClient:    customersv1.NewConsentRecordLifecycleServiceClient(deps.Conn),
	}
}

// GeneratePartnerIntegrationReport generates a partner integration.
func (s *PartnerIntegrationReportingServer) GeneratePartnerIntegrationReport(ctx context.Context, req *partnerapiv1.GeneratePartnerIntegrationReportRequest) (*partnerapiv1.GeneratePartnerIntegrationReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_integration", req.GetPartnerIntegrationId(), "generate_partner_integration_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate partner integration report", "tenant", req.GetContext().GetTenantId())
	explainPartnerWebhookDecisionRequest := &partnerapiv1.ExplainPartnerWebhookDecisionRequest{
		Context: req.GetContext(),
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{PartnerIntegrationId: req.GetPartnerIntegrationId()},
		},
		Details: &partnerapiv1.ExplainPartnerWebhookDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		Annotations: req.GetAnnotations(),
	}
	explainPartnerWebhookDecisionReply := new(partnerapiv1.ExplainPartnerWebhookDecisionResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.partnerapi.v1.PartnerWebhookValidationService/ExplainPartnerWebhookDecision", explainPartnerWebhookDecisionRequest, explainPartnerWebhookDecisionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/ExplainPartnerWebhookDecision")
	}
	cloned := proto.Clone(req.GetDetails()).(*partnerapiv1.GeneratePartnerIntegrationReportRequest_Details)
	summarizeMetricSeriesesRequest := &metricsv1.SummarizeMetricSeriesesRequest{
		Context: req.GetContext(),
		Details: &metricsv1.SummarizeMetricSeriesesRequest_Details{Format: cloned.GetFormat()},
		Reason:  req.GetReason(),
	}
	summarizeMetricSeriesesReply, err := s.summarizeMetricSeriesesFn(ctx, summarizeMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/SummarizeMetricSerieses")
	}
	rejectPartnerOrderRequest := &partnerapiv1.RejectPartnerOrderRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.RejectPartnerOrderRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: explainPartnerWebhookDecisionReply.GetPartnerWebhook().GetSubject().GetDisplayName(),
				UserId:      req.GetContext().GetActor().GetUserId(),
			},
		},
		Reason: req.GetReason(),
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Counterparty: &sellersv1.SellerRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	type rejectPartnerOrderResult struct {
		reply *partnerapiv1.RejectPartnerOrderResponse
		err   error
	}
	rejectPartnerOrderResults := make(chan rejectPartnerOrderResult, 1)
	go func() {
		reply, err := s.partnerOrderApprovalClient.RejectPartnerOrder(ctx, rejectPartnerOrderRequest)
		rejectPartnerOrderResults <- rejectPartnerOrderResult{reply, err}
	}()
	var rejectPartnerOrderReply *partnerapiv1.RejectPartnerOrderResponse
	select {
	case result := <-rejectPartnerOrderResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.partnerapi.v1.PartnerOrderApprovalService/RejectPartnerOrder")
		}
		rejectPartnerOrderReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &partnerapiv1.GeneratePartnerIntegrationReportResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: explainPartnerWebhookDecisionReply.GetResult().GetCorrelationId(),
			Details:       rejectPartnerOrderReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		Summary:  summarizeMetricSeriesesReply.GetSummary(),
		ReportId: summarizeMetricSeriesesReply.GetReportId(),
		Period:   req.GetDetails().GetPeriod(),
	}, nil
}

// FetchPartnerIntegrationReport returns a partner integration.
func (s *PartnerIntegrationReportingServer) FetchPartnerIntegrationReport(ctx context.Context, req *partnerapiv1.FetchPartnerIntegrationReportRequest) (*partnerapiv1.FetchPartnerIntegrationReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_integration", req.GetPartnerIntegrationId(), "fetch_partner_integration_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch partner integration report", "tenant", req.GetContext().GetTenantId())
	delegateFulfillmentExceptionApprovalRequest := &fulfillmentv1.DelegateFulfillmentExceptionApprovalRequest{
		Context: req.GetContext(),
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Assignee: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}
	delegateFulfillmentExceptionApprovalReply := new(fulfillmentv1.DelegateFulfillmentExceptionApprovalResponse)
	if err := grpcx.Invoke(ctx, s.conn, fulfillmentv1.FulfillmentExceptionApprovalService_DelegateFulfillmentExceptionApproval_FullMethodName, delegateFulfillmentExceptionApprovalRequest, delegateFulfillmentExceptionApprovalReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentExceptionApprovalService/DelegateFulfillmentExceptionApproval")
	}
	return &partnerapiv1.FetchPartnerIntegrationReportResponse{
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Audit: &commonv1.AuditInfo{
				CreatedBy: delegateFulfillmentExceptionApprovalReply.GetFulfillmentException().GetAudit().GetCreatedBy(),
			},
			PartnerIntegrationId: req.GetPartnerIntegrationId(),
		},
		Result: &commonv1.OperationResult{
			Details: delegateFulfillmentExceptionApprovalReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// SchedulePartnerIntegrationReport schedules a partner integration.
func (s *PartnerIntegrationReportingServer) SchedulePartnerIntegrationReport(ctx context.Context, req *partnerapiv1.SchedulePartnerIntegrationReportRequest) (*partnerapiv1.SchedulePartnerIntegrationReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SchedulePartnerIntegrationReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_integration", req.GetPartnerIntegrationId(), "schedule_partner_integration_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule partner integration report", "tenant", req.GetContext().GetTenantId())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	approvePartnerOrderRequest := &partnerapiv1.ApprovePartnerOrderRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.ApprovePartnerOrderRequest_Details{
			Approver: &identityv1.UserRef{UserId: cachedContext.GetActor().GetUserId()},
		},
		Annotations: annotations,
		Reason:      req.GetReason(),
	}
	approvePartnerOrderReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*partnerapiv1.ApprovePartnerOrderResponse, error) {
		return s.partnerOrderApprovalClient.ApprovePartnerOrder(ctx, approvePartnerOrderRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderApprovalService/ApprovePartnerOrder")
	}
	generateSellerScorecardReportRequest := &sellersv1.GenerateSellerScorecardReportRequest{
		Context: req.GetContext(),
		Details: &sellersv1.GenerateSellerScorecardReportRequest_Details{RecipientEmails: req.GetDetails().GetRecipientEmails()},
	}
	generateSellerScorecardReportReply, err := s.meteredSellerScorecardReporting.GenerateSellerScorecardReport(ctx, generateSellerScorecardReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardReportingService/GenerateSellerScorecardReport")
	}
	rejectPartnerOrderRequest := &partnerapiv1.RejectPartnerOrderRequest{
		Context: req.GetContext(),
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Counterparty: &sellersv1.SellerRef{
				DisplayName: approvePartnerOrderReply.GetPartnerOrder().GetCounterparty().GetDisplayName(),
			},
		},
	}
	rejectPartnerOrderReply, err := grpcx.NewClient(s.conn, partnerapiv1.NewPartnerOrderApprovalServiceClient).RejectPartnerOrder(ctx, rejectPartnerOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderApprovalService/RejectPartnerOrder")
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	revertSearchQueryRequest := &searchv1.RevertSearchQueryRequest{
		Context: req.GetContext(),
		SearchQuery: &searchv1.SearchQuery{
			Subject: &searchv1.SearchIndexRef{
				DisplayName: rejectPartnerOrderReply.GetPartnerOrder().GetCounterparty().GetDisplayName(),
			},
		},
		Details: &searchv1.RevertSearchQueryRequest_Details{Snapshot: snapshot},
	}
	revertSearchQueryReply, err := s.searchQueryHistoryPort.RevertSearchQuery(ctx, revertSearchQueryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryHistoryService/RevertSearchQuery")
	}
	_ = revertSearchQueryReply
	return &partnerapiv1.SchedulePartnerIntegrationReportResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: approvePartnerOrderReply.GetResult().GetCorrelationId(),
			ReferenceIds:  approvePartnerOrderReply.GetResult().GetReferenceIds(),
			Message:       rejectPartnerOrderReply.GetOutcome().GetMessage(),
			Status:        "ok",
		},
		ReportId: generateSellerScorecardReportReply.GetReportId(),
		Summary:  generateSellerScorecardReportReply.GetSummary(),
		Period:   req.GetDetails().GetPeriod(),
	}, nil
}

// ListPartnerIntegrationReports lists partner integrations a page at a time.
func (s *PartnerIntegrationReportingServer) ListPartnerIntegrationReports(ctx context.Context, req *partnerapiv1.ListPartnerIntegrationReportsRequest) (*partnerapiv1.ListPartnerIntegrationReportsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListPartnerIntegrationReports", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list partner integration reports", "tenant", req.GetContext().GetTenantId())
	listPendingPartnerOrdersRequest := &partnerapiv1.ListPendingPartnerOrdersRequest{}
	listPendingPartnerOrdersRequest.Context = req.GetContext()
	listPendingPartnerOrdersRequest.Filter = &partnerapiv1.ListPendingPartnerOrdersRequest_Filter{CreatedAfter: req.GetFilter().GetCreatedAfter()}
	listPendingPartnerOrdersRequest.Page = &commonv1.PageRequest{PageToken: req.GetPage().GetPageToken()}
	listPendingPartnerOrdersReply, err := s.partnerOrderApprovalClient.ListPendingPartnerOrders(ctx, listPendingPartnerOrdersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderApprovalService/ListPendingPartnerOrders")
	}
	return &partnerapiv1.ListPartnerIntegrationReportsResponse{
		Result: &commonv1.OperationResult{
			Message: listPendingPartnerOrdersReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// SharePartnerIntegrationReport shares a partner integration.
func (s *PartnerIntegrationReportingServer) SharePartnerIntegrationReport(ctx context.Context, req *partnerapiv1.SharePartnerIntegrationReportRequest) (*partnerapiv1.SharePartnerIntegrationReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_integration", req.GetPartnerIntegrationId(), "share_partner_integration_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share partner integration report", "tenant", req.GetContext().GetTenantId())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "partnerapi.share_partner_integration_report.approve_partner_order",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"partner_order.counterparty.email": req.GetDetails().GetRecipient().GetEmail(),
			"context.tenant_id":                req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	commentTierApprovalRequest := &loyaltyv1.CommentTierApprovalRequest{
		Context: req.GetContext(),
		Tier: &loyaltyv1.Tier{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		TierId: envelope.GetDisplayName(),
	}
	send := s.commentTierApprovalSender(commentTierApprovalRequest)
	commentTierApprovalReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierApprovalService/CommentTierApproval")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope2 partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	scheduleConsentRecordExpiryRequest := &customersv1.ScheduleConsentRecordExpiryRequest{
		Context: req.GetContext(),
		Details: &customersv1.ScheduleConsentRecordExpiryRequest_Details{
			Notify: &commonv1.ContactInfo{
				Name:  req.GetDetails().GetRecipient().GetName(),
				Phone: req.GetDetails().GetRecipient().GetPhone(),
			},
		},
		ConsentRecord:   &customersv1.ConsentRecord{Detail: packed},
		ConsentRecordId: envelope2.GetDisplayName(),
	}
	send2 := s.scheduleConsentRecordExpirySender(scheduleConsentRecordExpiryRequest)
	scheduleConsentRecordExpiryReply, err := send2(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordLifecycleService/ScheduleConsentRecordExpiry")
	}
	return &partnerapiv1.SharePartnerIntegrationReportResponse{
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Contact: &commonv1.ContactInfo{
				Email: commentTierApprovalReply.GetTier().GetOwner().GetEmail(),
				Phone: req.GetDetails().GetRecipient().GetPhone(),
			},
			Holder: &partnerapiv1.PartnerWebhookRef{
				DisplayName: scheduleConsentRecordExpiryReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
			PartnerIntegrationId: req.GetPartnerIntegrationId(),
		},
		Result: &commonv1.OperationResult{
			Details: scheduleConsentRecordExpiryReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// CountPartnerIntegrationTotals counts partner integrations by status.
func (s *PartnerIntegrationReportingServer) CountPartnerIntegrationTotals(ctx context.Context, req *partnerapiv1.CountPartnerIntegrationTotalsRequest) (*partnerapiv1.CountPartnerIntegrationTotalsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CountPartnerIntegrationTotals", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "count partner integration totals", "tenant", req.GetContext().GetTenantId())
	listPendingPartnerOrdersRequest := buildListPendingPartnerOrdersRequestForCountPartnerIntegrationTotals(req)
	listPendingPartnerOrdersReply, err := s.partnerOrderApprovalClient.ListPendingPartnerOrders(ctx, listPendingPartnerOrdersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderApprovalService/ListPendingPartnerOrders")
	}
	return &partnerapiv1.CountPartnerIntegrationTotalsResponse{
		Result: &commonv1.OperationResult{
			Message: listPendingPartnerOrdersReply.GetResult().GetMessage(),
			Details: listPendingPartnerOrdersReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// DeletePartnerIntegrationReport deletes a partner integration.
func (s *PartnerIntegrationReportingServer) DeletePartnerIntegrationReport(ctx context.Context, req *partnerapiv1.DeletePartnerIntegrationReportRequest) (*partnerapiv1.DeletePartnerIntegrationReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_integration", req.GetPartnerIntegrationId(), "delete_partner_integration_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete partner integration report", "tenant", req.GetContext().GetTenantId())
	scoreSynonymRequest := &searchv1.ScoreSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{Etag: req.GetEtag()},
			Owner: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
		Reason: req.GetReason(),
	}
	scoreSynonymReply := new(searchv1.ScoreSynonymResponse)
	if err := s.conn.Invoke(ctx, searchSynonymValidationServiceScoreSynonymMethod, scoreSynonymRequest, scoreSynonymReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/ScoreSynonym")
	}
	escalatePartnerOrderRequest := &partnerapiv1.EscalatePartnerOrderRequest{}
	escalatePartnerOrderRequest.Context = req.GetContext()
	escalatePartnerOrderRequest.PartnerOrder = &partnerapiv1.PartnerOrder{
		Lines: []*partnerapiv1.PartnerOrderLine{&partnerapiv1.PartnerOrderLine{
			Tax: &partnerapiv1.PartnerOrderLineTax{
				Amount: scoreSynonymReply.GetSynonym().GetDiscount().GetAmount(),
			},
		}},
		Counterparty: &sellersv1.SellerRef{
			DisplayName: scoreSynonymReply.GetSynonym().GetOwner().GetDisplayName(),
			Email:       scoreSynonymReply.GetSynonym().GetOwner().GetEmail(),
		},
		Audit: &commonv1.AuditInfo{
			CreatedAt: scoreSynonymReply.GetSynonym().GetAudit().GetCreatedAt(),
		},
	}
	escalatePartnerOrderReply, err := s.partnerOrderApprovalClient.EscalatePartnerOrder(ctx, escalatePartnerOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderApprovalService/EscalatePartnerOrder")
	}
	return &partnerapiv1.DeletePartnerIntegrationReportResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  scoreSynonymReply.GetResult().GetReferenceIds(),
			CorrelationId: escalatePartnerOrderReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// commentTierApprovalSender returns a function that sends one request to
// acme.loyalty.v1.TierApprovalService/CommentTierApproval.
func (s *PartnerIntegrationReportingServer) commentTierApprovalSender(request *loyaltyv1.CommentTierApprovalRequest) func(context.Context) (*loyaltyv1.CommentTierApprovalResponse, error) {
	return func(ctx context.Context) (*loyaltyv1.CommentTierApprovalResponse, error) {
		return s.tierApprovalClient.CommentTierApproval(ctx, request)
	}
}

// scheduleConsentRecordExpirySender returns a function that sends one request to
// acme.customers.v1.ConsentRecordLifecycleService/ScheduleConsentRecordExpiry.
func (s *PartnerIntegrationReportingServer) scheduleConsentRecordExpirySender(request *customersv1.ScheduleConsentRecordExpiryRequest) func(context.Context) (*customersv1.ScheduleConsentRecordExpiryResponse, error) {
	return func(ctx context.Context) (*customersv1.ScheduleConsentRecordExpiryResponse, error) {
		return s.consentRecordLifecycleClient.ScheduleConsentRecordExpiry(ctx, request)
	}
}
