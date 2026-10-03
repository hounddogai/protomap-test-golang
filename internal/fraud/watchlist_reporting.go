package fraud

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// WatchlistReportingServer implements `acme.fraud.v1.WatchlistReportingService`.
// Builds and schedules reports about watchlists.
type WatchlistReportingServer struct {
	fraudv1.UnimplementedWatchlistReportingServiceServer
	store                            *Store
	log                              *slog.Logger
	customerSegmentCalculationClient customersv1.CustomerSegmentCalculationServiceClient
	reviewModerationWorkflowClient   reviewsv1.ReviewModerationWorkflowServiceClient
	riskAssessmentClient             fraudv1.RiskAssessmentServiceClient
	fraudRuleValidationClient        fraudv1.FraudRuleValidationServiceClient
	fraudRuleCalculationClient       fraudv1.FraudRuleCalculationServiceClient
	featureFlagValidationClient      featureflagsv1.FeatureFlagValidationServiceClient
	customerSegmentValidationClient  customersv1.CustomerSegmentValidationServiceClient
	stockReservationWorkflowClient   inventoryv1.StockReservationWorkflowServiceClient
	conn                             *grpc.ClientConn
	clients                          *registry.Registry
}

// NewWatchlistReportingServer wires a WatchlistReportingServer.
func NewWatchlistReportingServer(deps platform.Deps) *WatchlistReportingServer {
	return &WatchlistReportingServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.fraud.v1.WatchlistReportingService"),
		customerSegmentCalculationClient: customersv1.NewCustomerSegmentCalculationServiceClient(deps.Conn),
		reviewModerationWorkflowClient:   reviewsv1.NewReviewModerationWorkflowServiceClient(deps.Conn),
		riskAssessmentClient:             fraudv1.NewRiskAssessmentServiceClient(deps.Conn),
		fraudRuleValidationClient:        fraudv1.NewFraudRuleValidationServiceClient(deps.Conn),
		fraudRuleCalculationClient:       fraudv1.NewFraudRuleCalculationServiceClient(deps.Conn),
		featureFlagValidationClient:      featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn),
		customerSegmentValidationClient:  customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn),
		stockReservationWorkflowClient:   inventoryv1.NewStockReservationWorkflowServiceClient(deps.Conn),
		conn:                             deps.Conn,
		clients:                          deps.Clients,
	}
}

// GenerateWatchlistReport generates a watchlist.
func (s *WatchlistReportingServer) GenerateWatchlistReport(ctx context.Context, req *fraudv1.GenerateWatchlistReportRequest) (*fraudv1.GenerateWatchlistReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GenerateWatchlistReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "watchlist", req.GetWatchlistId(), "generate_watchlist_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate watchlist report", "tenant", req.GetContext().GetTenantId())
	recalculateCustomerSegmentsRequest := buildRecalculateCustomerSegmentsRequestForGenerateWatchlistReport(req)
	recalculateCustomerSegmentsReply, err := s.customerSegmentCalculationClient.RecalculateCustomerSegments(ctx, recalculateCustomerSegmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/RecalculateCustomerSegments")
	}
	cancelReviewModerationWorkflowRequest := buildCancelReviewModerationWorkflowRequestForGenerateWatchlistReport(req, recalculateCustomerSegmentsReply)
	cancelReviewModerationWorkflowReply, err := s.reviewModerationWorkflowClient.CancelReviewModerationWorkflow(ctx, cancelReviewModerationWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationWorkflowService/CancelReviewModerationWorkflow")
	}
	return &fraudv1.GenerateWatchlistReportResponse{
		Result: &commonv1.OperationResult{
			Status:       recalculateCustomerSegmentsReply.GetResult().GetStatus(),
			Details:      cancelReviewModerationWorkflowReply.GetResult().GetDetails(),
			ReferenceIds: cancelReviewModerationWorkflowReply.GetResult().GetReferenceIds(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchWatchlistReport returns a watchlist.
func (s *WatchlistReportingServer) FetchWatchlistReport(ctx context.Context, req *fraudv1.FetchWatchlistReportRequest) (*fraudv1.FetchWatchlistReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "watchlist", req.GetWatchlistId(), "fetch_watchlist_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch watchlist report", "tenant", req.GetContext().GetTenantId())
	batchGetRiskAssessmentsRequest := &fraudv1.BatchGetRiskAssessmentsRequest{Context: req.GetContext()}
	send := s.batchGetRiskAssessmentsSender(batchGetRiskAssessmentsRequest)
	batchGetRiskAssessmentsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentService/BatchGetRiskAssessments")
	}
	explainFraudRuleDecisionRequest := newExplainFraudRuleDecisionRequest(withExplainFraudRuleDecisionContext(req.GetContext()), withExplainFraudRuleDecisionFraudRule(&fraudv1.FraudRule{
		Owner: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
	}), withExplainFraudRuleDecisionDetails(&fraudv1.ExplainFraudRuleDecisionRequest_Details{Locale: req.GetContext().GetLocale()}))
	explainFraudRuleDecisionReply, err := s.fraudRuleValidationClient.ExplainFraudRuleDecision(ctx, explainFraudRuleDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudRuleValidationService/ExplainFraudRuleDecision")
	}
	return &fraudv1.FetchWatchlistReportResponse{
		Result: &commonv1.OperationResult{
			Details:       batchGetRiskAssessmentsReply.GetResult().GetDetails(),
			CorrelationId: explainFraudRuleDecisionReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Audit: &commonv1.AuditInfo{
					CreatedAt: explainFraudRuleDecisionReply.GetFraudRule().GetAudit().GetCreatedAt(),
				},
			}},
			WatchlistId: req.GetWatchlistId(),
		},
	}, nil
}

// ScheduleWatchlistReport schedules a watchlist.
func (s *WatchlistReportingServer) ScheduleWatchlistReport(ctx context.Context, req *fraudv1.ScheduleWatchlistReportRequest) (*fraudv1.ScheduleWatchlistReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "watchlist", req.GetWatchlistId(), "schedule_watchlist_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule watchlist report", "tenant", req.GetContext().GetTenantId())
	calculateFraudRuleRequest := buildCalculateFraudRuleRequestForScheduleWatchlistReport(req)
	calculateFraudRuleReply, err := s.fraudRuleCalculationClient.CalculateFraudRule(ctx, calculateFraudRuleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudRuleCalculationService/CalculateFraudRule")
	}
	previewFeatureFlagReply, err := s.featureFlagValidationClient.PreviewFeatureFlag(ctx, &featureflagsv1.PreviewFeatureFlagRequest{
		Context: req.GetContext(),
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Action: &featureflagsv1.FeatureFlag_Route{Route: &featureflagsv1.FeatureFlagRouteAction{
				Weight: calculateFraudRuleReply.GetFraudRule().GetRoute().GetWeight(),
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/PreviewFeatureFlag")
	}
	_ = previewFeatureFlagReply
	return &fraudv1.ScheduleWatchlistReportResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: calculateFraudRuleReply.GetResult().GetReferenceIds(),
			Message:      calculateFraudRuleReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// StreamWatchlistMetrics streams the changes of watchlists as they happen.
func (s *WatchlistReportingServer) StreamWatchlistMetrics(req *fraudv1.StreamWatchlistMetricsRequest, stream grpc.ServerStreamingServer[fraudv1.StreamWatchlistMetricsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected StreamWatchlistMetrics", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "stream watchlist metrics", "tenant", req.GetContext().GetTenantId())
	validateCustomerSegmentRequest := buildValidateCustomerSegmentRequestForStreamWatchlistMetrics(req)
	validateCustomerSegmentReply, err := s.customerSegmentValidationClient.ValidateCustomerSegment(ctx, validateCustomerSegmentRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/ValidateCustomerSegment")
	}
	step := &watchlistReportingServerStreamWatchlistMetricsResumeStockReservationWorkflowStep{request: &inventoryv1.ResumeStockReservationWorkflowRequest{
		Context: req.GetContext(),
		StockReservation: &inventoryv1.StockReservation{
			Participants: []*inventoryv1.StockReservationParticipant{&inventoryv1.StockReservationParticipant{
				Contact: &commonv1.ContactInfo{
					Email: validateCustomerSegmentReply.GetCustomerSegment().GetOwner().GetEmail(),
				},
			}},
			Organizer: &identityv1.UserRef{
				UserId: validateCustomerSegmentReply.GetCustomerSegment().GetOwner().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: validateCustomerSegmentReply.GetCustomerSegment().GetAudit().GetUpdatedAt(),
				UpdatedBy: validateCustomerSegmentReply.GetCustomerSegment().GetAudit().GetUpdatedBy(),
			},
		},
	}}
	if err := s.runStreamWatchlistMetricsResumeStockReservationWorkflow(ctx, step); err != nil {
		return grpcx.Wrap(err, "acme.inventory.v1.StockReservationWorkflowService/ResumeStockReservationWorkflow")
	}
	resumeStockReservationWorkflowReply := step.reply
	if err := stream.Send(&fraudv1.StreamWatchlistMetricsResponse{
		Watchlist: &fraudv1.Watchlist{
			Audit: &commonv1.AuditInfo{
				CreatedAt: validateCustomerSegmentReply.GetCustomerSegment().GetAudit().GetCreatedAt(),
			},
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Manager: &identityv1.UserRef{
					DisplayName: validateCustomerSegmentReply.GetCustomerSegment().GetOwner().GetDisplayName(),
				},
			}},
			Location: &commonv1.PostalAddress{
				PostalCode: resumeStockReservationWorkflowReply.GetStockReservation().GetLocation().GetPostalCode(),
				Region:     req.GetFilter().GetRegion(),
			},
			Manager: &identityv1.UserRef{Email: req.GetSubscriber().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}); err != nil {
		return err
	}
	return nil
}

// ListWatchlistReports lists watchlists a page at a time.
func (s *WatchlistReportingServer) ListWatchlistReports(ctx context.Context, req *fraudv1.ListWatchlistReportsRequest) (*fraudv1.ListWatchlistReportsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list watchlist reports", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	rejectUsageRecordRequest := &subscriptionsv1.RejectUsageRecordRequest{
		Context: req.GetContext(),
		UsageRecord: &subscriptionsv1.UsageRecord{
			Subject:      &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
			Measurements: []*subscriptionsv1.UsageRecordMeasurement{&subscriptionsv1.UsageRecordMeasurement{ObservedAt: req.GetFilter().GetCreatedAfter()}},
			Source:       req.GetFilter().GetOwnerId(),
			Detail:       packed,
		},
		Details: &subscriptionsv1.RejectUsageRecordRequest_Details{
			Approver: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}
	rejectUsageRecordReply := new(subscriptionsv1.RejectUsageRecordResponse)
	if err := grpc.Invoke(ctx, "/acme.subscriptions.v1.UsageRecordApprovalService/RejectUsageRecord", rejectUsageRecordRequest, rejectUsageRecordReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordApprovalService/RejectUsageRecord")
	}
	return &fraudv1.ListWatchlistReportsResponse{
		Watchlists: []*fraudv1.Watchlist{&fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Manager: &identityv1.UserRef{
						Email: rejectUsageRecordReply.GetUsageRecord().GetSubject().GetEmail(),
					},
				}},
			}},
		}},
		Result: &commonv1.OperationResult{
			ReferenceIds: rejectUsageRecordReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// CountWatchlistTotals counts watchlists by status.
func (s *WatchlistReportingServer) CountWatchlistTotals(ctx context.Context, req *fraudv1.CountWatchlistTotalsRequest) (*fraudv1.CountWatchlistTotalsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "count watchlist totals", "tenant", req.GetContext().GetTenantId())
	return &fraudv1.CountWatchlistTotalsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// DeleteWatchlistReport deletes a watchlist.
func (s *WatchlistReportingServer) DeleteWatchlistReport(ctx context.Context, req *fraudv1.DeleteWatchlistReportRequest) (*fraudv1.DeleteWatchlistReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "watchlist", req.GetWatchlistId(), "delete_watchlist_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete watchlist report", "tenant", req.GetContext().GetTenantId())
	customerCalculation, ok := s.clients.Get("acme.customers.v1.CustomerCalculationService").(customersv1.CustomerCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.customers.v1.CustomerCalculationService")
	}
	simulateCustomerRequest := &customersv1.SimulateCustomerRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
	}
	simulateCustomerReply, err := customerCalculation.SimulateCustomer(ctx, simulateCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/SimulateCustomer")
	}
	return &fraudv1.DeleteWatchlistReportResponse{
		Result: &commonv1.OperationResult{
			Message: simulateCustomerReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// batchGetRiskAssessmentsSender returns a function that sends one request to
// acme.fraud.v1.RiskAssessmentService/BatchGetRiskAssessments.
func (s *WatchlistReportingServer) batchGetRiskAssessmentsSender(request *fraudv1.BatchGetRiskAssessmentsRequest) func(context.Context) (*fraudv1.BatchGetRiskAssessmentsResponse, error) {
	return func(ctx context.Context) (*fraudv1.BatchGetRiskAssessmentsResponse, error) {
		return s.riskAssessmentClient.BatchGetRiskAssessments(ctx, request)
	}
}

// watchlistReportingServerStreamWatchlistMetricsResumeStockReservationWorkflowStep carries the request of one step of
// StreamWatchlistMetrics and, once it ran, its reply.
type watchlistReportingServerStreamWatchlistMetricsResumeStockReservationWorkflowStep struct {
	request *inventoryv1.ResumeStockReservationWorkflowRequest
	reply   *inventoryv1.ResumeStockReservationWorkflowResponse
}

func (s *WatchlistReportingServer) runStreamWatchlistMetricsResumeStockReservationWorkflow(ctx context.Context, step *watchlistReportingServerStreamWatchlistMetricsResumeStockReservationWorkflowStep) error {
	reply, err := s.stockReservationWorkflowClient.ResumeStockReservationWorkflow(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
