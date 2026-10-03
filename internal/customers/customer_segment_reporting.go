package customers

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	insurancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/insurance/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	recommendationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/recommendations/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// CustomerSegmentReportingServer implements `acme.customers.v1.CustomerSegmentReportingService`.
// Builds and schedules reports about customer segments.
type CustomerSegmentReportingServer struct {
	customersv1.UnimplementedCustomerSegmentReportingServiceServer
	store                        *Store
	log                          *slog.Logger
	clients                      *registry.Registry
	customerValidationClient     customersv1.CustomerValidationServiceClient
	conn                         *grpc.ClientConn
	returnRequestApprovalClient  returnsv1.ReturnRequestApprovalServiceClient
	meteredSavedListApproval     meteredSavedListApproval
	resumeSellerWorkflowFn       func(context.Context, *sellersv1.ResumeSellerWorkflowRequest, ...grpc.CallOption) (*sellersv1.ResumeSellerWorkflowResponse, error)
	notificationValidationClient notificationsv1.NotificationValidationServiceClient
	preferenceValidationOnce     sync.Once
	preferenceValidationCached   customersv1.PreferenceValidationServiceClient
	auditEventValidationOnce     sync.Once
	auditEventValidationCached   auditv1.AuditEventValidationServiceClient
	metricSeriesClient           metricsv1.MetricSeriesServiceClient
}

// NewCustomerSegmentReportingServer wires a CustomerSegmentReportingServer.
func NewCustomerSegmentReportingServer(deps platform.Deps) *CustomerSegmentReportingServer {
	sellerWorkflow := sellersv1.NewSellerWorkflowServiceClient(deps.Conn)
	return &CustomerSegmentReportingServer{
		store:                        NewStore(deps.DB),
		log:                          deps.Logger.With("service", "acme.customers.v1.CustomerSegmentReportingService"),
		clients:                      deps.Clients,
		customerValidationClient:     customersv1.NewCustomerValidationServiceClient(deps.Conn),
		conn:                         deps.Conn,
		returnRequestApprovalClient:  returnsv1.NewReturnRequestApprovalServiceClient(deps.Conn),
		meteredSavedListApproval:     meteredSavedListApproval{SavedListApprovalServiceClient: cartv1.NewSavedListApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		resumeSellerWorkflowFn:       sellerWorkflow.ResumeSellerWorkflow,
		notificationValidationClient: notificationsv1.NewNotificationValidationServiceClient(deps.Conn),
		metricSeriesClient:           metricsv1.NewMetricSeriesServiceClient(deps.Conn),
	}
}

// GenerateCustomerSegmentReport generates a customer segment.
func (s *CustomerSegmentReportingServer) GenerateCustomerSegmentReport(ctx context.Context, req *customersv1.GenerateCustomerSegmentReportRequest) (*customersv1.GenerateCustomerSegmentReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer_segment", req.GetCustomerSegmentId(), "generate_customer_segment_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate customer segment report", "tenant", req.GetContext().GetTenantId())
	recommendationSet, ok := s.clients.Get("acme.recommendations.v1.RecommendationSetService").(recommendationsv1.RecommendationSetServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.recommendations.v1.RecommendationSetService")
	}
	updateRecommendationSetRequest := &recommendationsv1.UpdateRecommendationSetRequest{
		Context: req.GetContext(),
		RecommendationSet: &recommendationsv1.RecommendationSet{
			Counterparty: &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	updateRecommendationSetReply, err := recommendationSet.UpdateRecommendationSet(ctx, updateRecommendationSetRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.recommendations.v1.RecommendationSetService/UpdateRecommendationSet")
	}
	explainCustomerDecisionRequest := &customersv1.ExplainCustomerDecisionRequest{}
	explainCustomerDecisionRequest.Context = req.GetContext()
	explainCustomerDecisionRequest.Customer = &customersv1.Customer{
		PrimaryAddress: &commonv1.PostalAddress{
			Recipient: updateRecommendationSetReply.GetRecommendationSet().GetBillingAddress().GetRecipient(),
			Region:    updateRecommendationSetReply.GetRecommendationSet().GetBillingAddress().GetRegion(),
		},
		Audit: &commonv1.AuditInfo{
			CreatedBy: updateRecommendationSetReply.GetRecommendationSet().GetAudit().GetCreatedBy(),
		},
	}
	explainCustomerDecisionReply, err := s.customerValidationClient.ExplainCustomerDecision(ctx, explainCustomerDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/ExplainCustomerDecision")
	}
	syncPaymentInstrumentsRequest := &paymentsv1.SyncPaymentInstrumentsRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Contact: &commonv1.ContactInfo{
				Email: explainCustomerDecisionReply.GetCustomer().GetContact().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: explainCustomerDecisionReply.GetCustomer().GetAudit().GetUpdatedBy(),
			},
			Funding: &paymentsv1.PaymentInstrument_Bank{Bank: &paymentsv1.PaymentInstrumentBankFunding{
				AccountHolder: updateRecommendationSetReply.GetRecommendationSet().GetBank().GetAccountHolder(),
			}},
		},
	}
	syncPaymentInstrumentsReply := new(paymentsv1.SyncPaymentInstrumentsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", paymentsPaymentInstrumentSyncServiceName, "SyncPaymentInstruments"), syncPaymentInstrumentsRequest, syncPaymentInstrumentsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentSyncService/SyncPaymentInstruments")
	}
	return &customersv1.GenerateCustomerSegmentReportResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: updateRecommendationSetReply.GetResult().GetCorrelationId(),
			ReferenceIds:  explainCustomerDecisionReply.GetResult().GetReferenceIds(),
			Details:       explainCustomerDecisionReply.GetResult().GetDetails(),
			Message:       syncPaymentInstrumentsReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchCustomerSegmentReport returns a customer segment.
func (s *CustomerSegmentReportingServer) FetchCustomerSegmentReport(ctx context.Context, req *customersv1.FetchCustomerSegmentReportRequest) (*customersv1.FetchCustomerSegmentReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer_segment", req.GetCustomerSegmentId(), "fetch_customer_segment_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch customer segment report", "tenant", req.GetContext().GetTenantId())
	policy, ok := s.clients.Get("acme.insurance.v1.PolicyService").(insurancev1.PolicyServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.insurance.v1.PolicyService")
	}
	updatePolicyRequest := &insurancev1.UpdatePolicyRequest{
		Context: req.GetContext(),
		Policy: &insurancev1.Policy{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	updatePolicyReply, err := policy.UpdatePolicy(ctx, updatePolicyRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.insurance.v1.PolicyService/UpdatePolicy")
	}
	rejectReturnRequestRequest := buildRejectReturnRequestRequestForFetchCustomerSegmentReport(req, updatePolicyReply)
	rejectReturnRequestReply, err := s.returnRequestApprovalClient.RejectReturnRequest(ctx, rejectReturnRequestRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestApprovalService/RejectReturnRequest")
	}
	return &customersv1.FetchCustomerSegmentReportResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: updatePolicyReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				Email: rejectReturnRequestReply.GetReturnRequest().GetCounterparty().GetEmail(),
			},
			CustomerSegmentId: req.GetCustomerSegmentId(),
		},
	}, nil
}

// ScheduleCustomerSegmentReport schedules a customer segment.
func (s *CustomerSegmentReportingServer) ScheduleCustomerSegmentReport(ctx context.Context, req *customersv1.ScheduleCustomerSegmentReportRequest) (*customersv1.ScheduleCustomerSegmentReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScheduleCustomerSegmentReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer_segment", req.GetCustomerSegmentId(), "schedule_customer_segment_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule customer segment report", "tenant", req.GetContext().GetTenantId())
	delegateSavedListApprovalRequest := &cartv1.DelegateSavedListApprovalRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
	}
	delegateSavedListApprovalReply, err := s.meteredSavedListApproval.DelegateSavedListApproval(ctx, delegateSavedListApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListApprovalService/DelegateSavedListApproval")
	}
	return &customersv1.ScheduleCustomerSegmentReportResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: delegateSavedListApprovalReply.GetResult().GetCorrelationId(),
			ReferenceIds:  delegateSavedListApprovalReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// StreamCustomerSegmentMetrics streams the changes of customer segments as they happen.
func (s *CustomerSegmentReportingServer) StreamCustomerSegmentMetrics(req *customersv1.StreamCustomerSegmentMetricsRequest, stream grpc.ServerStreamingServer[customersv1.StreamCustomerSegmentMetricsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected StreamCustomerSegmentMetrics", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "stream customer segment metrics", "tenant", req.GetContext().GetTenantId())
	resumeSellerWorkflowRequest := &sellersv1.ResumeSellerWorkflowRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetSubscriber().GetPreferredChannel(),
				Name:             req.GetSubscriber().GetName(),
			},
			RelationshipManager: &identityv1.UserRef{Email: req.GetSubscriber().GetEmail()},
		},
		Details: &sellersv1.ResumeSellerWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Phone: req.GetSubscriber().GetPhone()},
		},
	}
	resumeSellerWorkflowReply, err := s.resumeSellerWorkflowFn(ctx, resumeSellerWorkflowRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.sellers.v1.SellerWorkflowService/ResumeSellerWorkflow")
	}
	recallSubscriptionPlanRequest := &subscriptionsv1.RecallSubscriptionPlanRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.RecallSubscriptionPlanRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: resumeSellerWorkflowReply.GetSeller().GetRelationshipManager().GetDisplayName(),
			},
		},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Audit: &commonv1.AuditInfo{
				Etag: resumeSellerWorkflowReply.GetSeller().GetAudit().GetEtag(),
			},
		},
	}
	recallSubscriptionPlanReply, err := recallSubscriptionPlanCall(ctx, recallSubscriptionPlanRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanApprovalService/RecallSubscriptionPlan")
	}
	if err := stream.Send(&customersv1.StreamCustomerSegmentMetricsResponse{
		Result: &commonv1.OperationResult{Status: resumeSellerWorkflowReply.GetOutcome().GetStatus()},
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				UserId: resumeSellerWorkflowReply.GetSeller().GetRelationshipManager().GetUserId(),
				Email:  req.GetSubscriber().GetEmail(),
			},
			Version: recallSubscriptionPlanReply.GetSubscriptionPlan().GetVersion(),
		},
	}); err != nil {
		return err
	}
	return nil
}

// ListCustomerSegmentReports lists customer segments a page at a time.
func (s *CustomerSegmentReportingServer) ListCustomerSegmentReports(ctx context.Context, req *customersv1.ListCustomerSegmentReportsRequest) (*customersv1.ListCustomerSegmentReportsResponse, error) {
	s.log.InfoContext(ctx, "list customer segment reports", "tenant", req.GetContext().GetTenantId())
	scoreNotificationRequest := buildScoreNotificationRequestForListCustomerSegmentReports(req)
	scoreNotificationReply, err := s.notificationValidationClient.ScoreNotification(ctx, scoreNotificationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationValidationService/ScoreNotification")
	}
	explainPreferenceDecisionRequest := &customersv1.ExplainPreferenceDecisionRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				Email: scoreNotificationReply.GetNotification().GetRequester().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: scoreNotificationReply.GetNotification().GetAudit().GetCreatedBy(),
			},
		},
	}
	explainPreferenceDecisionReply, err := s.preferenceValidationLazy().ExplainPreferenceDecision(ctx, explainPreferenceDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/ExplainPreferenceDecision")
	}
	return &customersv1.ListCustomerSegmentReportsResponse{
		Result: &commonv1.OperationResult{
			Message: scoreNotificationReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		CustomerSegments: []*customersv1.CustomerSegment{&customersv1.CustomerSegment{
			Effective: explainPreferenceDecisionReply.GetPreference().GetEffective(),
		}},
	}, nil
}

// ShareCustomerSegmentReport shares a customer segment.
func (s *CustomerSegmentReportingServer) ShareCustomerSegmentReport(ctx context.Context, req *customersv1.ShareCustomerSegmentReportRequest) (*customersv1.ShareCustomerSegmentReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer_segment", req.GetCustomerSegmentId(), "share_customer_segment_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share customer segment report", "tenant", req.GetContext().GetTenantId())
	var envelope customersv1.CustomerRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	lintAuditEventsRequest := &auditv1.LintAuditEventsRequest{
		Context: req.GetContext(),
		Details: &auditv1.LintAuditEventsRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetRecipient().GetPhone(),
				Email: req.GetDetails().GetRecipient().GetEmail(),
				Name:  req.GetDetails().GetRecipient().GetName(),
			},
		},
		Annotations:  req.GetAnnotations(),
		AuditEventId: envelope.GetDisplayName(),
	}
	lintAuditEventsReply, err := s.auditEventValidationLazy().LintAuditEvents(ctx, lintAuditEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventValidationService/LintAuditEvents")
	}
	return &customersv1.ShareCustomerSegmentReportResponse{
		Result: &commonv1.OperationResult{Status: lintAuditEventsReply.GetResult().GetStatus()},
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				DisplayName: lintAuditEventsReply.GetAuditEvent().GetSubject().GetDisplayName(),
				Email:       req.GetDetails().GetRecipient().GetEmail(),
			},
			CustomerSegmentId: req.GetCustomerSegmentId(),
		},
	}, nil
}

// CountCustomerSegmentTotals counts customer segments by status.
func (s *CustomerSegmentReportingServer) CountCustomerSegmentTotals(ctx context.Context, req *customersv1.CountCustomerSegmentTotalsRequest) (*customersv1.CountCustomerSegmentTotalsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "count customer segment totals", "tenant", req.GetContext().GetTenantId())
	commentSavedListApprovalRequest := &cartv1.CommentSavedListApprovalRequest{
		Context: req.GetContext(),
		SavedList: &cartv1.SavedList{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Details: &cartv1.CommentSavedListApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
	}
	commentSavedListApprovalReply := new(cartv1.CommentSavedListApprovalResponse)
	if err := s.conn.Invoke(ctx, cartSavedListApprovalServiceCommentSavedListApprovalMethod, commentSavedListApprovalRequest, commentSavedListApprovalReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListApprovalService/CommentSavedListApproval")
	}
	return &customersv1.CountCustomerSegmentTotalsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: commentSavedListApprovalReply.GetResult().GetCorrelationId(),
			ReferenceIds:  commentSavedListApprovalReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
	}, nil
}

// DeleteCustomerSegmentReport deletes a customer segment.
func (s *CustomerSegmentReportingServer) DeleteCustomerSegmentReport(ctx context.Context, req *customersv1.DeleteCustomerSegmentReportRequest) (*customersv1.DeleteCustomerSegmentReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer_segment", req.GetCustomerSegmentId(), "delete_customer_segment_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete customer segment report", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	listMetricSeriesesRequest := &metricsv1.ListMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter: &metricsv1.ListMetricSeriesesRequest_Filter{
			OwnerId:      req.GetCustomerSegmentId(),
			CreatedAfter: cloned.GetDeadline(),
			Labels:       baggage,
		},
	}
	type listMetricSeriesesResult struct {
		reply *metricsv1.ListMetricSeriesesResponse
		err   error
	}
	listMetricSeriesesResults := make(chan listMetricSeriesesResult, 1)
	go func() {
		reply, err := s.metricSeriesClient.ListMetricSerieses(ctx, listMetricSeriesesRequest)
		listMetricSeriesesResults <- listMetricSeriesesResult{reply, err}
	}()
	var listMetricSeriesesReply *metricsv1.ListMetricSeriesesResponse
	select {
	case result := <-listMetricSeriesesResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.metrics.v1.MetricSeriesService/ListMetricSerieses")
		}
		listMetricSeriesesReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &customersv1.DeleteCustomerSegmentReportResponse{
		Result: &commonv1.OperationResult{
			Details: listMetricSeriesesReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// preferenceValidationLazy dials acme.customers.v1.PreferenceValidationService the first time a handler needs it.
func (s *CustomerSegmentReportingServer) preferenceValidationLazy() customersv1.PreferenceValidationServiceClient {
	s.preferenceValidationOnce.Do(func() { s.preferenceValidationCached = customersv1.NewPreferenceValidationServiceClient(s.conn) })
	return s.preferenceValidationCached
}

// auditEventValidationLazy dials acme.audit.v1.AuditEventValidationService the first time a handler needs it.
func (s *CustomerSegmentReportingServer) auditEventValidationLazy() auditv1.AuditEventValidationServiceClient {
	s.auditEventValidationOnce.Do(func() { s.auditEventValidationCached = auditv1.NewAuditEventValidationServiceClient(s.conn) })
	return s.auditEventValidationCached
}
