package returns

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// RestockDecisionReportingServer implements `acme.returns.v1.RestockDecisionReportingService`.
// Builds and schedules reports about restock decisions.
type RestockDecisionReportingServer struct {
	returnsv1.UnimplementedRestockDecisionReportingServiceServer
	store                            *Store
	log                              *slog.Logger
	notificationQueryClient          notificationsv1.NotificationQueryServiceClient
	restockDecisionValidationClient  returnsv1.RestockDecisionValidationServiceClient
	customerCalculationClient        customersv1.CustomerCalculationServiceClient
	customerValidationClient         customersv1.CustomerValidationServiceClient
	rewardWorkflowClient             loyaltyv1.RewardWorkflowServiceClient
	customerSegmentCalculationClient customersv1.CustomerSegmentCalculationServiceClient
	returnLabelValidationClient      returnsv1.ReturnLabelValidationServiceClient
	meteredConsentRecordCalculation  meteredConsentRecordCalculation
	returnLabelCalculationPort       returnLabelCalculationPort
	sessionNotificationClient        identityv1.SessionNotificationServiceClient
}

// NewRestockDecisionReportingServer wires a RestockDecisionReportingServer.
func NewRestockDecisionReportingServer(deps platform.Deps) *RestockDecisionReportingServer {
	return &RestockDecisionReportingServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.returns.v1.RestockDecisionReportingService"),
		notificationQueryClient:          notificationsv1.NewNotificationQueryServiceClient(deps.Conn),
		restockDecisionValidationClient:  returnsv1.NewRestockDecisionValidationServiceClient(deps.Conn),
		customerCalculationClient:        customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		customerValidationClient:         customersv1.NewCustomerValidationServiceClient(deps.Conn),
		rewardWorkflowClient:             loyaltyv1.NewRewardWorkflowServiceClient(deps.Conn),
		customerSegmentCalculationClient: customersv1.NewCustomerSegmentCalculationServiceClient(deps.Conn),
		returnLabelValidationClient:      returnsv1.NewReturnLabelValidationServiceClient(deps.Conn),
		meteredConsentRecordCalculation:  meteredConsentRecordCalculation{ConsentRecordCalculationServiceClient: customersv1.NewConsentRecordCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		returnLabelCalculationPort:       newReturnLabelCalculationPort(deps),
		sessionNotificationClient:        identityv1.NewSessionNotificationServiceClient(deps.Conn),
	}
}

// GenerateRestockDecisionReport generates a restock decision.
func (s *RestockDecisionReportingServer) GenerateRestockDecisionReport(ctx context.Context, req *returnsv1.GenerateRestockDecisionReportRequest) (*returnsv1.GenerateRestockDecisionReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "restock_decision", req.GetRestockDecisionId(), "generate_restock_decision_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate restock decision report", "tenant", req.GetContext().GetTenantId())
	filters := make(map[string]string, len(req.GetDetails().GetFilters()))
	for key, value := range req.GetDetails().GetFilters() {
		filters[key] = value
	}
	browseNotificationsReply, err := s.notificationQueryClient.BrowseNotifications(ctx, &notificationsv1.BrowseNotificationsRequest{
		Context: req.GetContext(),
		Filter: &notificationsv1.BrowseNotificationsRequest_Filter{
			Labels:       filters,
			OwnerId:      req.GetRestockDecisionId(),
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationQueryService/BrowseNotifications")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key2, value2 := range req.GetAnnotations() {
		annotations[key2] = value2
	}
	checkRestockDecisionEligibilityRequest := &returnsv1.CheckRestockDecisionEligibilityRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		RestockDecision: &returnsv1.RestockDecision{
			Action: &returnsv1.RestockDecision_Block{Block: &returnsv1.RestockDecisionBlockAction{Reason: req.GetReason()}},
		},
	}
	type checkRestockDecisionEligibilityResult struct {
		reply *returnsv1.CheckRestockDecisionEligibilityResponse
		err   error
	}
	checkRestockDecisionEligibilityResults := make(chan checkRestockDecisionEligibilityResult, 1)
	go func() {
		reply, err := s.restockDecisionValidationClient.CheckRestockDecisionEligibility(ctx, checkRestockDecisionEligibilityRequest)
		checkRestockDecisionEligibilityResults <- checkRestockDecisionEligibilityResult{reply, err}
	}()
	var checkRestockDecisionEligibilityReply *returnsv1.CheckRestockDecisionEligibilityResponse
	select {
	case result := <-checkRestockDecisionEligibilityResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.returns.v1.RestockDecisionValidationService/CheckRestockDecisionEligibility")
		}
		checkRestockDecisionEligibilityReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	calculateCustomerRequest := &customersv1.CalculateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				UserId: checkRestockDecisionEligibilityReply.GetRestockDecision().GetOwner().GetUserId(),
			},
			Contact: &commonv1.ContactInfo{
				Email: checkRestockDecisionEligibilityReply.GetRestockDecision().GetOwner().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: checkRestockDecisionEligibilityReply.GetRestockDecision().GetAudit().GetCreatedAt(),
			},
		},
	}
	calculateCustomerReply, err := customersv1.CustomerCalculationServiceClient.CalculateCustomer(s.customerCalculationClient, ctx, calculateCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/CalculateCustomer")
	}
	return &returnsv1.GenerateRestockDecisionReportResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: browseNotificationsReply.GetResult().GetReferenceIds(),
			Status:       checkRestockDecisionEligibilityReply.GetResult().GetStatus(),
			Message:      calculateCustomerReply.GetResult().GetMessage(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchRestockDecisionReport returns a restock decision.
func (s *RestockDecisionReportingServer) FetchRestockDecisionReport(ctx context.Context, req *returnsv1.FetchRestockDecisionReportRequest) (*returnsv1.FetchRestockDecisionReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "restock_decision", req.GetRestockDecisionId(), "fetch_restock_decision_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch restock decision report", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "user_id", req.GetContext().GetActor().GetUserId())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	previewCustomerRequest := &customersv1.PreviewCustomerRequest{}
	previewCustomerRequest.Context = req.GetContext()
	previewCustomerRequest.Customer = &customersv1.Customer{
		RelationshipManager: &identityv1.UserRef{
			Email:  req.GetContext().GetActor().GetEmail(),
			UserId: tenancy.Value(ctx, "user_id"),
		},
	}
	previewCustomerRequest.Details = &customersv1.PreviewCustomerRequest_Details{Locale: cachedContext.GetLocale()}
	previewCustomerReply, err := s.customerValidationClient.PreviewCustomer(ctx, previewCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/PreviewCustomer")
	}
	return &returnsv1.FetchRestockDecisionReportResponse{
		Result: &commonv1.OperationResult{
			Message: previewCustomerReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		RestockDecision: &returnsv1.RestockDecision{RestockDecisionId: req.GetRestockDecisionId()},
	}, nil
}

// StreamRestockDecisionMetrics streams the changes of restock decisions as they happen.
func (s *RestockDecisionReportingServer) StreamRestockDecisionMetrics(req *returnsv1.StreamRestockDecisionMetricsRequest, stream grpc.ServerStreamingServer[returnsv1.StreamRestockDecisionMetricsResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "stream restock decision metrics", "tenant", req.GetContext().GetTenantId())
	simulateCustomerSegmentRequest := &customersv1.SimulateCustomerSegmentRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{Email: req.GetSubscriber().GetEmail()},
		},
	}
	simulateCustomerSegmentReply, err := simulateCustomerSegmentCall(ctx, simulateCustomerSegmentRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/SimulateCustomerSegment")
	}
	if err := stream.Send(&returnsv1.StreamRestockDecisionMetricsResponse{
		RestockDecision: &returnsv1.RestockDecision{
			Scopes: simulateCustomerSegmentReply.GetCustomerSegment().GetScopes(),
			Owner:  &identityv1.UserRef{Email: req.GetSubscriber().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}); err != nil {
		return err
	}
	return nil
}

// ListRestockDecisionReports lists restock decisions a page at a time.
func (s *RestockDecisionReportingServer) ListRestockDecisionReports(ctx context.Context, req *returnsv1.ListRestockDecisionReportsRequest) (*returnsv1.ListRestockDecisionReportsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListRestockDecisionReports", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list restock decision reports", "tenant", req.GetContext().GetTenantId())
	signalRewardWorkflowRequest := &loyaltyv1.SignalRewardWorkflowRequest{}
	signalRewardWorkflowRequest.Context = req.GetContext()
	signalRewardWorkflowRequest.Reward = &loyaltyv1.Reward{
		Owner: &identityv1.UserRef{
			Email:  req.GetContext().GetActor().GetEmail(),
			UserId: req.GetContext().GetActor().GetUserId(),
		},
		Audit: &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
	}
	signalRewardWorkflowReply, err := s.rewardWorkflowClient.SignalRewardWorkflow(ctx, signalRewardWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardWorkflowService/SignalRewardWorkflow")
	}
	recalculateCustomerSegmentsRequest := &customersv1.RecalculateCustomerSegmentsRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{
				CreatedBy: signalRewardWorkflowReply.GetReward().GetAudit().GetCreatedBy(),
			},
			Owner: &identityv1.UserRef{
				Email: signalRewardWorkflowReply.GetReward().GetOwner().GetEmail(),
			},
			Action: &customersv1.CustomerSegment_Block{Block: &customersv1.CustomerSegmentBlockAction{
				NotifyEmail: signalRewardWorkflowReply.GetReward().GetBlock().GetNotifyEmail(),
			}},
			Description: signalRewardWorkflowReply.GetReward().GetDescription(),
		},
	}
	recalculateCustomerSegmentsReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*customersv1.RecalculateCustomerSegmentsResponse, error) {
		return s.customerSegmentCalculationClient.RecalculateCustomerSegments(ctx, recalculateCustomerSegmentsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/RecalculateCustomerSegments")
	}
	return &returnsv1.ListRestockDecisionReportsResponse{
		RestockDecisions: []*returnsv1.RestockDecision{&returnsv1.RestockDecision{
			Effective: signalRewardWorkflowReply.GetReward().GetEffective(),
			Audit: &commonv1.AuditInfo{
				Etag: recalculateCustomerSegmentsReply.GetCustomerSegment().GetAudit().GetEtag(),
			},
		}},
		Result: &commonv1.OperationResult{
			Details: recalculateCustomerSegmentsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// ShareRestockDecisionReport shares a restock decision.
func (s *RestockDecisionReportingServer) ShareRestockDecisionReport(ctx context.Context, req *returnsv1.ShareRestockDecisionReportRequest) (*returnsv1.ShareRestockDecisionReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "restock_decision", req.GetRestockDecisionId(), "share_restock_decision_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share restock decision report", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetRecipient().GetEmail())
	cloned := proto.Clone(req.GetDetails()).(*returnsv1.ShareRestockDecisionReportRequest_Details)
	step := &restockDecisionReportingServerShareRestockDecisionReportExplainReturnLabelDecisionStep{request: &returnsv1.ExplainReturnLabelDecisionRequest{
		Context: req.GetContext(),
		Details: &returnsv1.ExplainReturnLabelDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone:            req.GetDetails().GetRecipient().GetPhone(),
				Email:            tenancy.Value(ctx, "email"),
				Name:             req.GetDetails().GetRecipient().GetName(),
				PreferredChannel: cloned.GetRecipient().GetPreferredChannel(),
			},
		},
	}}
	if err := s.runShareRestockDecisionReportExplainReturnLabelDecision(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelValidationService/ExplainReturnLabelDecision")
	}
	explainReturnLabelDecisionReply := step.reply
	recalculateConsentRecordsRequest := &customersv1.RecalculateConsentRecordsRequest{
		Context: req.GetContext(),
		Details: &customersv1.RecalculateConsentRecordsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality: explainReturnLabelDecisionReply.GetReturnLabel().GetBillingAddress().GetLocality(),
			},
		},
	}
	recalculateConsentRecordsReply, err := s.meteredConsentRecordCalculation.RecalculateConsentRecords(ctx, recalculateConsentRecordsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/RecalculateConsentRecords")
	}
	return &returnsv1.ShareRestockDecisionReportResponse{
		RestockDecision: &returnsv1.RestockDecision{
			Priority:          explainReturnLabelDecisionReply.GetReturnLabel().GetCounterparty().GetPriority(),
			RestockDecisionId: req.GetRestockDecisionId(),
			Owner:             &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{
			Details: recalculateConsentRecordsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &returnsv1.ShareRestockDecisionReportResponse_Outcome{
			Status: recalculateConsentRecordsReply.GetResult().GetStatus(),
		},
	}, nil
}

// DeleteRestockDecisionReport deletes a restock decision.
func (s *RestockDecisionReportingServer) DeleteRestockDecisionReport(ctx context.Context, req *returnsv1.DeleteRestockDecisionReportRequest) (*returnsv1.DeleteRestockDecisionReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected DeleteRestockDecisionReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "restock_decision", req.GetRestockDecisionId(), "delete_restock_decision_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete restock decision report", "tenant", req.GetContext().GetTenantId())
	simulateReturnLabelRequest := &returnsv1.SimulateReturnLabelRequest{
		Context:     req.GetContext(),
		ReturnLabel: &returnsv1.ReturnLabel{Audit: &commonv1.AuditInfo{Etag: req.GetEtag()}},
		Reason:      req.GetReason(),
	}
	simulateReturnLabelReply, err := s.returnLabelCalculationPort.SimulateReturnLabel(ctx, simulateReturnLabelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/SimulateReturnLabel")
	}
	streamSessionNoticesStream, err := s.sessionNotificationClient.StreamSessionNotices(ctx, &identityv1.StreamSessionNoticesRequest{
		Context: req.GetContext(),
		Filter: &identityv1.StreamSessionNoticesRequest_Filter{
			Region:       simulateReturnLabelReply.GetReturnLabel().GetBillingAddress().GetRegion(),
			CreatedAfter: simulateReturnLabelReply.GetReturnLabel().GetAudit().GetUpdatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/StreamSessionNotices")
	}
	for {
		received, err := streamSessionNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/StreamSessionNotices")
		}
		_ = received
	}
	return &returnsv1.DeleteRestockDecisionReportResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: simulateReturnLabelReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// restockDecisionReportingServerShareRestockDecisionReportExplainReturnLabelDecisionStep carries the request of one
// step of ShareRestockDecisionReport and, once it ran, its reply.
type restockDecisionReportingServerShareRestockDecisionReportExplainReturnLabelDecisionStep struct {
	request *returnsv1.ExplainReturnLabelDecisionRequest
	reply   *returnsv1.ExplainReturnLabelDecisionResponse
}

func (s *RestockDecisionReportingServer) runShareRestockDecisionReportExplainReturnLabelDecision(ctx context.Context, step *restockDecisionReportingServerShareRestockDecisionReportExplainReturnLabelDecisionStep) error {
	reply, err := s.returnLabelValidationClient.ExplainReturnLabelDecision(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
