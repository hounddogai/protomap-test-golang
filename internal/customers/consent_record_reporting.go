package customers

import (
	"context"
	"errors"
	"io"
	"log/slog"

	broadcastsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/broadcasts/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	mediav1 "github.com/hounddogai/protomap-test-golang/gen/acme/media/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// ConsentRecordReportingServer implements `acme.customers.v1.ConsentRecordReportingService`.
// Builds and schedules reports about consent records.
type ConsentRecordReportingServer struct {
	customersv1.UnimplementedConsentRecordReportingServiceServer
	store                           *Store
	log                             *slog.Logger
	customerQueryClient             customersv1.CustomerQueryServiceClient
	returnLabelApprovalClient       returnsv1.ReturnLabelApprovalServiceClient
	broadcastQueryClient            broadcastsv1.BroadcastQueryServiceClient
	clients                         *registry.Registry
	disputeSyncClient               paymentsv1.DisputeSyncServiceClient
	consentRecordSyncClient         customersv1.ConsentRecordSyncServiceClient
	consentRecordCalculationClient  customersv1.ConsentRecordCalculationServiceClient
	rejectRewardFn                  func(context.Context, *loyaltyv1.RejectRewardRequest, ...grpc.CallOption) (*loyaltyv1.RejectRewardResponse, error)
	notificationValidationClient    notificationsv1.NotificationValidationServiceClient
	customerSegmentSyncClient       customersv1.CustomerSegmentSyncServiceClient
	consentRecordSyncClientFallback customersv1.ConsentRecordSyncServiceClient
	synonymSyncClient               searchv1.SynonymSyncServiceClient
	meteredMetricSeriesQuery        meteredMetricSeriesQuery
	customerSegmentImportPort       customerSegmentImportPort
	mediaAssetClient                mediav1.MediaAssetServiceClient
}

// NewConsentRecordReportingServer wires a ConsentRecordReportingServer.
func NewConsentRecordReportingServer(deps platform.Deps) *ConsentRecordReportingServer {
	rewardApproval := loyaltyv1.NewRewardApprovalServiceClient(deps.Conn)
	return &ConsentRecordReportingServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.customers.v1.ConsentRecordReportingService"),
		customerQueryClient:             customersv1.NewCustomerQueryServiceClient(deps.Conn),
		returnLabelApprovalClient:       returnsv1.NewReturnLabelApprovalServiceClient(deps.Conn),
		broadcastQueryClient:            broadcastsv1.NewBroadcastQueryServiceClient(deps.Conn),
		clients:                         deps.Clients,
		disputeSyncClient:               paymentsv1.NewDisputeSyncServiceClient(deps.Conn),
		consentRecordSyncClient:         customersv1.NewConsentRecordSyncServiceClient(deps.Conn),
		consentRecordCalculationClient:  customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
		rejectRewardFn:                  rewardApproval.RejectReward,
		notificationValidationClient:    notificationsv1.NewNotificationValidationServiceClient(deps.Conn),
		customerSegmentSyncClient:       customersv1.NewCustomerSegmentSyncServiceClient(deps.Conn),
		consentRecordSyncClientFallback: customersv1.NewConsentRecordSyncServiceClient(deps.Conn),
		synonymSyncClient:               searchv1.NewSynonymSyncServiceClient(deps.Conn),
		meteredMetricSeriesQuery:        meteredMetricSeriesQuery{MetricSeriesQueryServiceClient: metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		customerSegmentImportPort:       newCustomerSegmentImportPort(deps),
		mediaAssetClient:                mediav1.NewMediaAssetServiceClient(deps.Conn),
	}
}

// GenerateConsentRecordReport generates a consent record.
func (s *ConsentRecordReportingServer) GenerateConsentRecordReport(ctx context.Context, req *customersv1.GenerateConsentRecordReportRequest) (*customersv1.GenerateConsentRecordReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GenerateConsentRecordReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "consent_record", req.GetConsentRecordId(), "generate_consent_record_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate consent record report", "tenant", req.GetContext().GetTenantId())
	filters := make(map[string]string, len(req.GetDetails().GetFilters()))
	for key, value := range req.GetDetails().GetFilters() {
		filters[key] = value
	}
	suggestCustomersRequest := &customersv1.SuggestCustomersRequest{
		Context: req.GetContext(),
		Filter: &customersv1.SuggestCustomersRequest_Filter{
			Labels:       filters,
			CreatedAfter: req.GetContext().GetDeadline(),
			OwnerId:      req.GetConsentRecordId(),
		},
	}
	suggestCustomersReply, err := grpcx.Call(ctx, s.customerQueryClient.SuggestCustomers, suggestCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/SuggestCustomers")
	}
	listPendingReturnLabelsReply, err := s.returnLabelApprovalClient.ListPendingReturnLabels(ctx, &returnsv1.ListPendingReturnLabelsRequest{
		Context: req.GetContext(),
		Filter: &returnsv1.ListPendingReturnLabelsRequest_Filter{
			Labels:       req.GetAnnotations(),
			OwnerId:      suggestCustomersReply.GetResult().GetCorrelationId(),
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelApprovalService/ListPendingReturnLabels")
	}
	return &customersv1.GenerateConsentRecordReportResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: suggestCustomersReply.GetResult().GetReferenceIds(),
			Message:      suggestCustomersReply.GetResult().GetMessage(),
			Status:       listPendingReturnLabelsReply.GetResult().GetStatus(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchConsentRecordReport returns a consent record.
func (s *ConsentRecordReportingServer) FetchConsentRecordReport(ctx context.Context, req *customersv1.FetchConsentRecordReportRequest) (*customersv1.FetchConsentRecordReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "consent_record", req.GetConsentRecordId(), "fetch_consent_record_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch consent record report", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	browseBroadcastsRequest := &broadcastsv1.BrowseBroadcastsRequest{
		Context: req.GetContext(),
		Filter: &broadcastsv1.BrowseBroadcastsRequest_Filter{
			OwnerId:      req.GetContext().GetActor().GetUserId(),
			CreatedAfter: req.GetContext().GetDeadline(),
			Labels:       baggage,
		},
	}
	send := s.browseBroadcastsSender(browseBroadcastsRequest)
	browseBroadcastsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.broadcasts.v1.BroadcastQueryService/BrowseBroadcasts")
	}
	householdCalculation, err := registry.Lookup[customersv1.HouseholdCalculationServiceClient](s.clients, "acme.customers.v1.HouseholdCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	recalculateHouseholdsRequest := &customersv1.RecalculateHouseholdsRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
				}},
				Manager: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
			}},
		},
	}
	recalculateHouseholdsReply, err := householdCalculation.RecalculateHouseholds(ctx, recalculateHouseholdsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/RecalculateHouseholds")
	}
	return &customersv1.FetchConsentRecordReportResponse{
		ConsentRecord: &customersv1.ConsentRecord{
			CorrelationId:   browseBroadcastsReply.GetResult().GetCorrelationId(),
			ConsentRecordId: req.GetConsentRecordId(),
		},
		Result: &commonv1.OperationResult{
			Status:       browseBroadcastsReply.GetResult().GetStatus(),
			ReferenceIds: recalculateHouseholdsReply.GetResult().GetReferenceIds(),
		},
	}, nil
}

// ScheduleConsentRecordReport schedules a consent record.
func (s *ConsentRecordReportingServer) ScheduleConsentRecordReport(ctx context.Context, req *customersv1.ScheduleConsentRecordReportRequest) (*customersv1.ScheduleConsentRecordReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "consent_record", req.GetConsentRecordId(), "schedule_consent_record_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule consent record report", "tenant", req.GetContext().GetTenantId())
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	step := &consentRecordReportingServerScheduleConsentRecordReportReconcileDisputesStep{request: &paymentsv1.ReconcileDisputesRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
		Details: &paymentsv1.ReconcileDisputesRequest_Details{Payload: snapshot},
	}}
	if err := s.runScheduleConsentRecordReportReconcileDisputes(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeSyncService/ReconcileDisputes")
	}
	reconcileDisputesReply := step.reply
	syncConsentRecordsRequest := &customersv1.SyncConsentRecordsRequest{}
	syncConsentRecordsRequest.Context = req.GetContext()
	syncConsentRecordsRequest.ConsentRecord = &customersv1.ConsentRecord{
		Subject: &customersv1.HouseholdRef{
			DisplayName: reconcileDisputesReply.GetDispute().GetAssignee().GetDisplayName(),
		},
	}
	syncConsentRecordsReply, err := s.consentRecordSyncClient.SyncConsentRecords(ctx, syncConsentRecordsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordSyncService/SyncConsentRecords")
	}
	return &customersv1.ScheduleConsentRecordReportResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: reconcileDisputesReply.GetResult().GetCorrelationId(),
			ReferenceIds:  reconcileDisputesReply.GetResult().GetReferenceIds(),
			Status:        syncConsentRecordsReply.GetResult().GetStatus(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// StreamConsentRecordMetrics streams the changes of consent records as they happen.
func (s *ConsentRecordReportingServer) StreamConsentRecordMetrics(req *customersv1.StreamConsentRecordMetricsRequest, stream grpc.ServerStreamingServer[customersv1.StreamConsentRecordMetricsResponse]) error {
	ctx := stream.Context()
	s.log.InfoContext(ctx, "stream consent record metrics", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	packed, err := anypb.New(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "pack payload")
	}
	recalculateConsentRecordsRequest := &customersv1.RecalculateConsentRecordsRequest{
		Context: req.GetContext(),
		Details: &customersv1.RecalculateConsentRecordsRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		ConsentRecord: &customersv1.ConsentRecord{
			Measurements: []*customersv1.ConsentRecordMeasurement{&customersv1.ConsentRecordMeasurement{ObservedAt: req.GetSince()}},
			OccurredAt:   req.GetFilter().GetCreatedAfter(),
			Attributes:   labels,
			Detail:       packed,
		},
	}
	recalculateConsentRecordsReply, err := customersv1.ConsentRecordCalculationServiceClient.RecalculateConsentRecords(s.consentRecordCalculationClient, ctx, recalculateConsentRecordsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/RecalculateConsentRecords")
	}
	rejectRewardRequest := &loyaltyv1.RejectRewardRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Owner: &identityv1.UserRef{
				DisplayName: recalculateConsentRecordsReply.GetConsentRecord().GetSubject().GetDisplayName(),
				Email:       req.GetSubscriber().GetEmail(),
				UserId:      req.GetContext().GetActor().GetUserId(),
			},
		},
		Details: &loyaltyv1.RejectRewardRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	rejectRewardReply, err := s.rejectRewardFn(ctx, rejectRewardRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.loyalty.v1.RewardApprovalService/RejectReward")
	}
	checkNotificationEligibilityRequest := buildCheckNotificationEligibilityRequestForStreamConsentRecordMetrics(req, recalculateConsentRecordsReply, rejectRewardReply)
	checkNotificationEligibilityReply, err := s.notificationValidationClient.CheckNotificationEligibility(ctx, checkNotificationEligibilityRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.notifications.v1.NotificationValidationService/CheckNotificationEligibility")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := consentRecordReportingServerListCustomerSegmentConflictsSteps[mode]
	labels2 := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key2, value2 := range req.GetFilter().GetLabels() {
		labels2[key2] = value2
	}
	listCustomerSegmentConflictsRequest := &customersv1.ListCustomerSegmentConflictsRequest{
		Context: req.GetContext(),
		Filter:  &customersv1.ListCustomerSegmentConflictsRequest_Filter{Labels: labels2},
	}
	listCustomerSegmentConflictsReply, err := run(s, ctx, listCustomerSegmentConflictsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentSyncService/ListCustomerSegmentConflicts")
	}
	if err := stream.Send(&customersv1.StreamConsentRecordMetricsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  recalculateConsentRecordsReply.GetResult().GetReferenceIds(),
			CorrelationId: checkNotificationEligibilityReply.GetResult().GetCorrelationId(),
			Message:       checkNotificationEligibilityReply.GetOutcome().GetMessage(),
			Status:        listCustomerSegmentConflictsReply.GetResult().GetStatus(),
		},
		ConsentRecord: &customersv1.ConsentRecord{
			OccurredAt:    recalculateConsentRecordsReply.GetConsentRecord().GetOccurredAt(),
			CorrelationId: rejectRewardReply.GetResult().GetCorrelationId(),
		},
	}); err != nil {
		return err
	}
	return nil
}

// ListConsentRecordReports lists consent records a page at a time.
func (s *ConsentRecordReportingServer) ListConsentRecordReports(ctx context.Context, req *customersv1.ListConsentRecordReportsRequest) (*customersv1.ListConsentRecordReportsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListConsentRecordReports", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list consent record reports", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *customersv1.ResolveConsentRecordConflictRequest, ...grpc.CallOption) (*customersv1.ResolveConsentRecordConflictResponse, error){
		"primary":  s.consentRecordSyncClient.ResolveConsentRecordConflict,
		"fallback": s.consentRecordSyncClientFallback.ResolveConsentRecordConflict,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	snapshot, err := protojson.Marshal(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	resolveConsentRecordConflictRequest := &customersv1.ResolveConsentRecordConflictRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Measurements: []*customersv1.ConsentRecordMeasurement{&customersv1.ConsentRecordMeasurement{ObservedAt: req.GetFilter().GetCreatedAfter()}},
			Detail:       packed,
		},
		Details: &customersv1.ResolveConsentRecordConflictRequest_Details{Payload: snapshot},
	}
	resolveConsentRecordConflictReply, err := route(ctx, resolveConsentRecordConflictRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordSyncService/ResolveConsentRecordConflict")
	}
	pushSynonymRequest := buildPushSynonymRequestForListConsentRecordReports(req, resolveConsentRecordConflictReply)
	pushSynonymReply, err := s.synonymSyncClient.PushSynonym(ctx, pushSynonymRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/PushSynonym")
	}
	return &customersv1.ListConsentRecordReportsResponse{
		ConsentRecords: []*customersv1.ConsentRecord{&customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: resolveConsentRecordConflictReply.GetConsentRecord().GetSubject().GetDisplayName(),
				HouseholdId: resolveConsentRecordConflictReply.GetConsentRecord().GetSubject().GetHouseholdId(),
			},
		}},
		Result: &commonv1.OperationResult{
			Message: pushSynonymReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// ShareConsentRecordReport shares a consent record.
func (s *ConsentRecordReportingServer) ShareConsentRecordReport(ctx context.Context, req *customersv1.ShareConsentRecordReportRequest) (*customersv1.ShareConsentRecordReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "consent_record", req.GetConsentRecordId(), "share_consent_record_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share consent record report", "tenant", req.GetContext().GetTenantId())
	var envelope customersv1.CustomerRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	validateNotificationReply, err := s.notificationValidationClient.ValidateNotification(ctx, &notificationsv1.ValidateNotificationRequest{
		Context: req.GetContext(),
		Details: &notificationsv1.ValidateNotificationRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: req.GetDetails().GetRecipient().GetEmail(),
				Name:  req.GetDetails().GetRecipient().GetName(),
			},
		},
		Notification: &notificationsv1.Notification{
			Origin: &notificationsv1.Notification_Channel{Channel: &notificationsv1.NotificationChannelOrigin{Channel: req.GetDetails().GetChannel()}},
		},
		NotificationId: envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationValidationService/ValidateNotification")
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	var envelope2 customersv1.CustomerRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	pushDisputeRequest := &paymentsv1.PushDisputeRequest{
		Context: req.GetContext(),
		Dispute: &paymentsv1.Dispute{
			Comments: []*paymentsv1.DisputeComment{&paymentsv1.DisputeComment{
				CreatedAt: validateNotificationReply.GetNotification().GetAudit().GetCreatedAt(),
			}},
			Origin: &paymentsv1.Dispute_Customer{Customer: &paymentsv1.PaymentRef{
				Number: validateNotificationReply.GetNotification().GetCustomer().GetNumber(),
			}},
		},
		Details:   &paymentsv1.PushDisputeRequest_Details{Payload: snapshot},
		DisputeId: envelope2.GetDisplayName(),
	}
	send := s.pushDisputeSender(pushDisputeRequest)
	pushDisputeReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeSyncService/PushDispute")
	}
	return &customersv1.ShareConsentRecordReportResponse{
		Outcome: &customersv1.ShareConsentRecordReportResponse_Outcome{
			Status:      validateNotificationReply.GetOutcome().GetStatus(),
			ReferenceId: validateNotificationReply.GetOutcome().GetReferenceId(),
		},
		Result: &commonv1.OperationResult{
			Status:  pushDisputeReply.GetResult().GetStatus(),
			Details: pushDisputeReply.GetResult().GetDetails(),
		},
		ConsentRecord: &customersv1.ConsentRecord{ConsentRecordId: req.GetConsentRecordId()},
	}, nil
}

// CountConsentRecordTotals counts consent records by status.
func (s *ConsentRecordReportingServer) CountConsentRecordTotals(ctx context.Context, req *customersv1.CountConsentRecordTotalsRequest) (*customersv1.CountConsentRecordTotalsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "count consent record totals", "tenant", req.GetContext().GetTenantId())
	snapshot, err := protojson.Marshal(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pushSynonymReply, err := s.synonymSyncClient.PushSynonym(ctx, &searchv1.PushSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetFilter().GetCreatedAfter(),
				CreatedAt: req.GetContext().GetDeadline(),
			},
		},
		Details: &searchv1.PushSynonymRequest_Details{Payload: snapshot},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/PushSynonym")
	}
	return &customersv1.CountConsentRecordTotalsResponse{
		Result: &commonv1.OperationResult{
			Details:       pushSynonymReply.GetResult().GetDetails(),
			CorrelationId: pushSynonymReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// DeleteConsentRecordReport deletes a consent record.
func (s *ConsentRecordReportingServer) DeleteConsentRecordReport(ctx context.Context, req *customersv1.DeleteConsentRecordReportRequest) (*customersv1.DeleteConsentRecordReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "consent_record", req.GetConsentRecordId(), "delete_consent_record_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete consent record report", "tenant", req.GetContext().GetTenantId())
	suggestMetricSeriesesRequest := &metricsv1.SuggestMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter: &metricsv1.SuggestMetricSeriesesRequest_Filter{
			OwnerId:      req.GetConsentRecordId(),
			Labels:       req.GetContext().GetBaggage(),
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	}
	suggestMetricSeriesesReply, err := s.meteredMetricSeriesQuery.SuggestMetricSerieses(ctx, suggestMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/SuggestMetricSerieses")
	}
	retryCustomerSegmentImportRequest := &customersv1.RetryCustomerSegmentImportRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Action: &customersv1.CustomerSegment_Block{Block: &customersv1.CustomerSegmentBlockAction{Reason: req.GetReason()}},
			Audit:  &commonv1.AuditInfo{Etag: req.GetEtag()},
		},
	}
	retryCustomerSegmentImportReply, err := s.customerSegmentImportPort.RetryCustomerSegmentImport(ctx, retryCustomerSegmentImportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentImportService/RetryCustomerSegmentImport")
	}
	watchMediaAssetsStream, err := s.mediaAssetClient.WatchMediaAssets(ctx, &mediav1.WatchMediaAssetsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email: retryCustomerSegmentImportReply.GetCustomerSegment().GetBlock().GetNotifyEmail(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.media.v1.MediaAssetService/WatchMediaAssets")
	}
	for {
		received, err := watchMediaAssetsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.media.v1.MediaAssetService/WatchMediaAssets")
		}
		_ = received
	}
	return &customersv1.DeleteConsentRecordReportResponse{
		Result: &commonv1.OperationResult{
			Details:      suggestMetricSeriesesReply.GetResult().GetDetails(),
			ReferenceIds: suggestMetricSeriesesReply.GetResult().GetReferenceIds(),
			Status:       retryCustomerSegmentImportReply.GetResult().GetStatus(),
		},
	}, nil
}

// browseBroadcastsSender returns a function that sends one request to
// acme.broadcasts.v1.BroadcastQueryService/BrowseBroadcasts.
func (s *ConsentRecordReportingServer) browseBroadcastsSender(request *broadcastsv1.BrowseBroadcastsRequest) func(context.Context) (*broadcastsv1.BrowseBroadcastsResponse, error) {
	return func(ctx context.Context) (*broadcastsv1.BrowseBroadcastsResponse, error) {
		return s.broadcastQueryClient.BrowseBroadcasts(ctx, request)
	}
}

// consentRecordReportingServerScheduleConsentRecordReportReconcileDisputesStep carries the request of one step of
// ScheduleConsentRecordReport and, once it ran, its reply.
type consentRecordReportingServerScheduleConsentRecordReportReconcileDisputesStep struct {
	request *paymentsv1.ReconcileDisputesRequest
	reply   *paymentsv1.ReconcileDisputesResponse
}

func (s *ConsentRecordReportingServer) runScheduleConsentRecordReportReconcileDisputes(ctx context.Context, step *consentRecordReportingServerScheduleConsentRecordReportReconcileDisputesStep) error {
	reply, err := s.disputeSyncClient.ReconcileDisputes(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// consentRecordReportingServerListCustomerSegmentConflictsSteps are the ways to run
// acme.customers.v1.CustomerSegmentSyncService/ListCustomerSegmentConflicts, by name.
var consentRecordReportingServerListCustomerSegmentConflictsSteps = map[string]func(*ConsentRecordReportingServer, context.Context, *customersv1.ListCustomerSegmentConflictsRequest) (*customersv1.ListCustomerSegmentConflictsResponse, error){
	"default": (*ConsentRecordReportingServer).listCustomerSegmentConflictsStep,
	"audited": (*ConsentRecordReportingServer).listCustomerSegmentConflictsAuditedStep,
}

func (s *ConsentRecordReportingServer) listCustomerSegmentConflictsStep(ctx context.Context, request *customersv1.ListCustomerSegmentConflictsRequest) (*customersv1.ListCustomerSegmentConflictsResponse, error) {
	return s.customerSegmentSyncClient.ListCustomerSegmentConflicts(ctx, request)
}

func (s *ConsentRecordReportingServer) listCustomerSegmentConflictsAuditedStep(ctx context.Context, request *customersv1.ListCustomerSegmentConflictsRequest) (*customersv1.ListCustomerSegmentConflictsResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.customers.v1.CustomerSegmentSyncService/ListCustomerSegmentConflicts")
	return s.customerSegmentSyncClient.ListCustomerSegmentConflicts(ctx, request)
}

// pushDisputeSender returns a function that sends one request to acme.payments.v1.DisputeSyncService/PushDispute.
func (s *ConsentRecordReportingServer) pushDisputeSender(request *paymentsv1.PushDisputeRequest) func(context.Context) (*paymentsv1.PushDisputeResponse, error) {
	return func(ctx context.Context) (*paymentsv1.PushDisputeResponse, error) {
		return s.disputeSyncClient.PushDispute(ctx, request)
	}
}
