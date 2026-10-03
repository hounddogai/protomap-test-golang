package sellers

import (
	"context"
	"log/slog"
	"sync"

	analyticsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/analytics/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// SellerScorecardReportingServer implements `acme.sellers.v1.SellerScorecardReportingService`.
// Builds and schedules reports about seller scorecards.
type SellerScorecardReportingServer struct {
	sellersv1.UnsafeSellerScorecardReportingServiceServer
	store                               *Store
	log                                 *slog.Logger
	giftCardBatchWorkflowClient         giftcardsv1.GiftCardBatchWorkflowServiceClient
	giftCardWorkflowClient              giftcardsv1.GiftCardWorkflowServiceClient
	giftCardWorkflowClientFallback      giftcardsv1.GiftCardWorkflowServiceClient
	conn                                *grpc.ClientConn
	giftCardTransactionWorkflowOnce     sync.Once
	giftCardTransactionWorkflowCached   giftcardsv1.GiftCardTransactionWorkflowServiceClient
	sellerPayoutScheduleReportingClient sellersv1.SellerPayoutScheduleReportingServiceClient
	giftCardBatchApprovalClient         giftcardsv1.GiftCardBatchApprovalServiceClient
	clients                             *registry.Registry
	consentRecordCalculationPort        consentRecordCalculationPort
	analyticsEventQueryClient           analyticsv1.AnalyticsEventQueryServiceClient
	giftCardApprovalClient              giftcardsv1.GiftCardApprovalServiceClient
	sellerScorecardValidationClient     sellersv1.SellerScorecardValidationServiceClient
	householdValidationClient           customersv1.HouseholdValidationServiceClient
	meteredCustomerSegmentValidation    meteredCustomerSegmentValidation
}

// NewSellerScorecardReportingServer wires a SellerScorecardReportingServer.
func NewSellerScorecardReportingServer(deps platform.Deps) *SellerScorecardReportingServer {
	return &SellerScorecardReportingServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.sellers.v1.SellerScorecardReportingService"),
		giftCardBatchWorkflowClient:         giftcardsv1.NewGiftCardBatchWorkflowServiceClient(deps.Conn),
		giftCardWorkflowClient:              giftcardsv1.NewGiftCardWorkflowServiceClient(deps.Conn),
		giftCardWorkflowClientFallback:      giftcardsv1.NewGiftCardWorkflowServiceClient(deps.Conn),
		conn:                                deps.Conn,
		sellerPayoutScheduleReportingClient: sellersv1.NewSellerPayoutScheduleReportingServiceClient(deps.Conn),
		giftCardBatchApprovalClient:         giftcardsv1.NewGiftCardBatchApprovalServiceClient(deps.Conn),
		clients:                             deps.Clients,
		consentRecordCalculationPort:        newConsentRecordCalculationPort(deps),
		analyticsEventQueryClient:           analyticsv1.NewAnalyticsEventQueryServiceClient(deps.Conn),
		giftCardApprovalClient:              giftcardsv1.NewGiftCardApprovalServiceClient(deps.Conn),
		sellerScorecardValidationClient:     sellersv1.NewSellerScorecardValidationServiceClient(deps.Conn),
		householdValidationClient:           customersv1.NewHouseholdValidationServiceClient(deps.Conn),
		meteredCustomerSegmentValidation:    meteredCustomerSegmentValidation{CustomerSegmentValidationServiceClient: customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}
}

// GenerateSellerScorecardReport generates a seller scorecard.
func (s *SellerScorecardReportingServer) GenerateSellerScorecardReport(ctx context.Context, req *sellersv1.GenerateSellerScorecardReportRequest) (*sellersv1.GenerateSellerScorecardReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_scorecard", req.GetSellerScorecardId(), "generate_seller_scorecard_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate seller scorecard report", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	retryGiftCardBatchWorkflowRequest := &giftcardsv1.RetryGiftCardBatchWorkflowRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
	}
	retryGiftCardBatchWorkflowReply, err := giftcardsv1.GiftCardBatchWorkflowServiceClient.RetryGiftCardBatchWorkflow(s.giftCardBatchWorkflowClient, ctx, retryGiftCardBatchWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchWorkflowService/RetryGiftCardBatchWorkflow")
	}
	routes := map[string]func(context.Context, *giftcardsv1.SignalGiftCardWorkflowRequest, ...grpc.CallOption) (*giftcardsv1.SignalGiftCardWorkflowResponse, error){
		"primary":  s.giftCardWorkflowClient.SignalGiftCardWorkflow,
		"fallback": s.giftCardWorkflowClientFallback.SignalGiftCardWorkflow,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	signalGiftCardWorkflowRequest := &giftcardsv1.SignalGiftCardWorkflowRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Funding: &giftcardsv1.GiftCard_Card{Card: &giftcardsv1.GiftCardCardFunding{
				Token: retryGiftCardBatchWorkflowReply.GetGiftCardBatch().GetCard().GetToken(),
				Last4: retryGiftCardBatchWorkflowReply.GetGiftCardBatch().GetCard().GetLast4(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: retryGiftCardBatchWorkflowReply.GetGiftCardBatch().GetAudit().GetCreatedAt(),
			},
			Holder: &giftcardsv1.GiftCardTransactionRef{
				DisplayName: retryGiftCardBatchWorkflowReply.GetGiftCardBatch().GetCounterparty().GetDisplayName(),
			},
		},
	}
	signalGiftCardWorkflowReply, err := route(ctx, signalGiftCardWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardWorkflowService/SignalGiftCardWorkflow")
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	resolveSellerScorecardConflictRequest := &sellersv1.ResolveSellerScorecardConflictRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			BillingAddress: &commonv1.PostalAddress{
				Recipient:   retryGiftCardBatchWorkflowReply.GetGiftCardBatch().GetBillingAddress().GetRecipient(),
				CountryCode: retryGiftCardBatchWorkflowReply.GetGiftCardBatch().GetBillingAddress().GetCountryCode(),
			},
			Settlement: &sellersv1.SellerScorecard_Card{Card: &sellersv1.SellerScorecardCardSettlement{
				Last4: signalGiftCardWorkflowReply.GetGiftCard().GetCard().GetLast4(),
			}},
			Counterparty: &customersv1.CustomerRef{
				DisplayName: retryGiftCardBatchWorkflowReply.GetGiftCardBatch().GetCounterparty().GetDisplayName(),
			},
		},
		Details: &sellersv1.ResolveSellerScorecardConflictRequest_Details{Payload: snapshot},
	}
	resolveSellerScorecardConflictReply, err := resolveSellerScorecardConflictCall(ctx, resolveSellerScorecardConflictRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardSyncService/ResolveSellerScorecardConflict")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	pauseGiftCardTransactionWorkflowRequest := &giftcardsv1.PauseGiftCardTransactionWorkflowRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: resolveSellerScorecardConflictReply.GetSellerScorecard().GetCounterparty().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	pauseGiftCardTransactionWorkflowReply, err := s.giftCardTransactionWorkflowLazy().PauseGiftCardTransactionWorkflow(ctx, pauseGiftCardTransactionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionWorkflowService/PauseGiftCardTransactionWorkflow")
	}
	return &sellersv1.GenerateSellerScorecardReportResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  retryGiftCardBatchWorkflowReply.GetResult().GetReferenceIds(),
			Message:       retryGiftCardBatchWorkflowReply.GetOutcome().GetMessage(),
			CorrelationId: pauseGiftCardTransactionWorkflowReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchSellerScorecardReport returns a seller scorecard.
func (s *SellerScorecardReportingServer) FetchSellerScorecardReport(ctx context.Context, req *sellersv1.FetchSellerScorecardReportRequest) (*sellersv1.FetchSellerScorecardReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_scorecard", req.GetSellerScorecardId(), "fetch_seller_scorecard_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch seller scorecard report", "tenant", req.GetContext().GetTenantId())
	return &sellersv1.FetchSellerScorecardReportResponse{
		SellerScorecard: &sellersv1.SellerScorecard{SellerScorecardId: req.GetSellerScorecardId()},
		Result:          &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScheduleSellerScorecardReport schedules a seller scorecard.
func (s *SellerScorecardReportingServer) ScheduleSellerScorecardReport(ctx context.Context, req *sellersv1.ScheduleSellerScorecardReportRequest) (*sellersv1.ScheduleSellerScorecardReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScheduleSellerScorecardReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_scorecard", req.GetSellerScorecardId(), "schedule_seller_scorecard_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule seller scorecard report", "tenant", req.GetContext().GetTenantId())
	filters := make(map[string]string, len(req.GetDetails().GetFilters()))
	for key, value := range req.GetDetails().GetFilters() {
		filters[key] = value
	}
	generateSellerPayoutScheduleReportRequest := &sellersv1.GenerateSellerPayoutScheduleReportRequest{
		Context: req.GetContext(),
		Details: &sellersv1.GenerateSellerPayoutScheduleReportRequest_Details{Filters: filters},
	}
	var generateSellerPayoutScheduleReportReply *sellersv1.GenerateSellerPayoutScheduleReportResponse
	if err := pipeline.Run(ctx, "acme.sellers.v1.SellerPayoutScheduleReportingService/GenerateSellerPayoutScheduleReport", func(ctx context.Context) error {
		answer, err := s.sellerPayoutScheduleReportingClient.GenerateSellerPayoutScheduleReport(ctx, generateSellerPayoutScheduleReportRequest)
		generateSellerPayoutScheduleReportReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleReportingService/GenerateSellerPayoutScheduleReport")
	}
	return &sellersv1.ScheduleSellerScorecardReportResponse{
		ReportId: generateSellerPayoutScheduleReportReply.GetReportId(),
		Period:   req.GetDetails().GetPeriod(),
		Result:   &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// StreamSellerScorecardMetrics streams the changes of seller scorecards as they happen.
func (s *SellerScorecardReportingServer) StreamSellerScorecardMetrics(req *sellersv1.StreamSellerScorecardMetricsRequest, stream grpc.ServerStreamingServer[sellersv1.StreamSellerScorecardMetricsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected StreamSellerScorecardMetrics", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "stream seller scorecard metrics", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetSubscriber())
	if err != nil {
		return grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.ContactInfo
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return err
	}
	delegateGiftCardBatchApprovalReply, err := s.giftCardBatchApprovalClient.DelegateGiftCardBatchApproval(ctx, &giftcardsv1.DelegateGiftCardBatchApprovalRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			IssuedAt:       req.GetSince(),
		},
		Details: &giftcardsv1.DelegateGiftCardBatchApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				Email:  (&decoded).GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchApprovalService/DelegateGiftCardBatchApproval")
	}
	tenantExport, err := registry.Lookup[identityv1.TenantExportServiceClient](s.clients, "acme.identity.v1.TenantExportService")
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	getTenantExportRequest := &identityv1.GetTenantExportRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
	}
	getTenantExportReply, err := tenantExport.GetTenantExport(ctx, getTenantExportRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.identity.v1.TenantExportService/GetTenantExport")
	}
	if err := stream.Send(&sellersv1.StreamSellerScorecardMetricsResponse{
		Result: &commonv1.OperationResult{
			Message: delegateGiftCardBatchApprovalReply.GetResult().GetMessage(),
			Details: delegateGiftCardBatchApprovalReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		SellerScorecard: &sellersv1.SellerScorecard{
			Lines: []*sellersv1.SellerScorecardLine{&sellersv1.SellerScorecardLine{
				Attributes: getTenantExportReply.GetTenant().GetAttributes(),
			}},
			Counterparty:   &customersv1.CustomerRef{Email: req.GetSubscriber().GetEmail()},
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}); err != nil {
		return err
	}
	return nil
}

// ListSellerScorecardReports lists seller scorecards a page at a time.
func (s *SellerScorecardReportingServer) ListSellerScorecardReports(ctx context.Context, req *sellersv1.ListSellerScorecardReportsRequest) (*sellersv1.ListSellerScorecardReportsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list seller scorecard reports", "tenant", req.GetContext().GetTenantId())
	return &sellersv1.ListSellerScorecardReportsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ShareSellerScorecardReport shares a seller scorecard.
func (s *SellerScorecardReportingServer) ShareSellerScorecardReport(ctx context.Context, req *sellersv1.ShareSellerScorecardReportRequest) (*sellersv1.ShareSellerScorecardReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_scorecard", req.GetSellerScorecardId(), "share_seller_scorecard_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share seller scorecard report", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope sellersv1.SellerRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	estimateConsentRecordRequest := &customersv1.EstimateConsentRecordRequest{
		Context:         req.GetContext(),
		Annotations:     annotations,
		Reason:          req.GetReason(),
		ConsentRecord:   &customersv1.ConsentRecord{Detail: packed},
		ConsentRecordId: envelope.GetDisplayName(),
	}
	estimateConsentRecordReply, err := s.consentRecordCalculationPort.EstimateConsentRecord(ctx, estimateConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/EstimateConsentRecord")
	}
	resolveAnalyticsEventsRequest := &analyticsv1.ResolveAnalyticsEventsRequest{Context: req.GetContext()}
	resolveAnalyticsEventsReply, err := grpcx.Call(ctx, s.analyticsEventQueryClient.ResolveAnalyticsEvents, resolveAnalyticsEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.analytics.v1.AnalyticsEventQueryService/ResolveAnalyticsEvents")
	}
	var envelope2 sellersv1.SellerRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	step := &sellerScorecardReportingServerShareSellerScorecardReportListPendingGiftCardsStep{request: &giftcardsv1.ListPendingGiftCardsRequest{
		Context: req.GetContext(),
		Filter: &giftcardsv1.ListPendingGiftCardsRequest_Filter{
			OwnerId:      estimateConsentRecordReply.GetConsentRecord().GetSubject().GetHouseholdId(),
			Labels:       req.GetDetails().GetVariables(),
			CreatedAfter: estimateConsentRecordReply.GetConsentRecord().GetOccurredAt(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope2.GetDisplayName()},
	}}
	if err := s.runShareSellerScorecardReportListPendingGiftCards(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardApprovalService/ListPendingGiftCards")
	}
	listPendingGiftCardsReply := step.reply
	var envelope3 sellersv1.SellerRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope3); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	lintSellerScorecardsReply, err := s.sellerScorecardValidationClient.LintSellerScorecards(ctx, &sellersv1.LintSellerScorecardsRequest{
		Context: req.GetContext(),
		Details: &sellersv1.LintSellerScorecardsRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetRecipient().GetName()},
		},
		SellerScorecardId: envelope3.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardValidationService/LintSellerScorecards")
	}
	return &sellersv1.ShareSellerScorecardReportResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: estimateConsentRecordReply.GetResult().GetCorrelationId(),
			Details:       resolveAnalyticsEventsReply.GetResult().GetDetails(),
			Status:        resolveAnalyticsEventsReply.GetResult().GetStatus(),
			Message:       listPendingGiftCardsReply.GetResult().GetMessage(),
			ReferenceIds:  lintSellerScorecardsReply.GetResult().GetReferenceIds(),
		},
		Outcome: &sellersv1.ShareSellerScorecardReportResponse_Outcome{
			Status:  estimateConsentRecordReply.GetResult().GetStatus(),
			Message: listPendingGiftCardsReply.GetResult().GetMessage(),
		},
		SellerScorecard: &sellersv1.SellerScorecard{
			BillingAddress: &commonv1.PostalAddress{
				Region: lintSellerScorecardsReply.GetSellerScorecard().GetBillingAddress().GetRegion(),
			},
			SellerScorecardId: req.GetSellerScorecardId(),
			Counterparty:      &customersv1.CustomerRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
	}, nil
}

// CountSellerScorecardTotals counts seller scorecards by status.
func (s *SellerScorecardReportingServer) CountSellerScorecardTotals(ctx context.Context, req *sellersv1.CountSellerScorecardTotalsRequest) (*sellersv1.CountSellerScorecardTotalsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "count seller scorecard totals", "tenant", req.GetContext().GetTenantId())
	return &sellersv1.CountSellerScorecardTotalsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// DeleteSellerScorecardReport deletes a seller scorecard.
func (s *SellerScorecardReportingServer) DeleteSellerScorecardReport(ctx context.Context, req *sellersv1.DeleteSellerScorecardReportRequest) (*sellersv1.DeleteSellerScorecardReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_scorecard", req.GetSellerScorecardId(), "delete_seller_scorecard_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete seller scorecard report", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	normalizeHouseholdRequest := &customersv1.NormalizeHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit:   &commonv1.AuditInfo{Etag: req.GetEtag()},
					Manager: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
				}},
			}},
			Manager: &identityv1.UserRef{Email: (&decoded).GetActor().GetEmail()},
		},
		Reason: req.GetReason(),
	}
	normalizeHouseholdReply, err := grpcx.Call(ctx, s.householdValidationClient.NormalizeHousehold, normalizeHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/NormalizeHousehold")
	}
	explainCustomerSegmentDecisionRequest := &customersv1.ExplainCustomerSegmentDecisionRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: normalizeHouseholdReply.GetHousehold().GetAudit().GetUpdatedBy(),
			},
			Owner: &identityv1.UserRef{
				UserId: normalizeHouseholdReply.GetHousehold().GetManager().GetUserId(),
			},
		},
	}
	explainCustomerSegmentDecisionReply, err := s.meteredCustomerSegmentValidation.ExplainCustomerSegmentDecision(ctx, explainCustomerSegmentDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/ExplainCustomerSegmentDecision")
	}
	return &sellersv1.DeleteSellerScorecardReportResponse{
		Result: &commonv1.OperationResult{
			Message: normalizeHouseholdReply.GetOutcome().GetMessage(),
			Details: normalizeHouseholdReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		EffectiveAt: explainCustomerSegmentDecisionReply.GetOutcome().GetEffectiveAt(),
	}, nil
}

// giftCardTransactionWorkflowLazy dials acme.giftcards.v1.GiftCardTransactionWorkflowService the first time a handler
// needs it.
func (s *SellerScorecardReportingServer) giftCardTransactionWorkflowLazy() giftcardsv1.GiftCardTransactionWorkflowServiceClient {
	s.giftCardTransactionWorkflowOnce.Do(func() {
		s.giftCardTransactionWorkflowCached = giftcardsv1.NewGiftCardTransactionWorkflowServiceClient(s.conn)
	})
	return s.giftCardTransactionWorkflowCached
}

// sellerScorecardReportingServerShareSellerScorecardReportListPendingGiftCardsStep carries the request of one step of
// ShareSellerScorecardReport and, once it ran, its reply.
type sellerScorecardReportingServerShareSellerScorecardReportListPendingGiftCardsStep struct {
	request *giftcardsv1.ListPendingGiftCardsRequest
	reply   *giftcardsv1.ListPendingGiftCardsResponse
}

func (s *SellerScorecardReportingServer) runShareSellerScorecardReportListPendingGiftCards(ctx context.Context, step *sellerScorecardReportingServerShareSellerScorecardReportListPendingGiftCardsStep) error {
	reply, err := s.giftCardApprovalClient.ListPendingGiftCards(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
