package customers

import (
	"context"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	concessionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/concessions/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sponsorshipsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sponsorships/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PreferenceReportingServer implements `acme.customers.v1.PreferenceReportingService`.
// Builds and schedules reports about preferences.
type PreferenceReportingServer struct {
	customersv1.UnimplementedPreferenceReportingServiceServer
	store                          *Store
	log                            *slog.Logger
	conn                           *grpc.ClientConn
	sessionNotificationClient      identityv1.SessionNotificationServiceClient
	auditEventValidationClient     auditv1.AuditEventValidationServiceClient
	concessionStandQueryClient     concessionsv1.ConcessionStandQueryServiceClient
	syncConsentRecordsFn           func(context.Context, *customersv1.SyncConsentRecordsRequest, ...grpc.CallOption) (*customersv1.SyncConsentRecordsResponse, error)
	priceOverrideWorkflowClient    pricingv1.PriceOverrideWorkflowServiceClient
	searchQueryNotificationClient  searchv1.SearchQueryNotificationServiceClient
	householdLifecycleClient       customersv1.HouseholdLifecycleServiceClient
	consentRecordSyncClient        customersv1.ConsentRecordSyncServiceClient
	meteredSponsorCalculation      meteredSponsorCalculation
	priceQuoteApprovalClient       pricingv1.PriceQuoteApprovalServiceClient
	preferenceImportClient         customersv1.PreferenceImportServiceClient
	subscriptionPlanApprovalClient subscriptionsv1.SubscriptionPlanApprovalServiceClient
	accessPolicyQueryClient        authorizationv1.AccessPolicyQueryServiceClient
	preferenceCalculationClient    customersv1.PreferenceCalculationServiceClient
	consentRecordSyncPort          consentRecordSyncPort
}

// NewPreferenceReportingServer wires a PreferenceReportingServer.
func NewPreferenceReportingServer(deps platform.Deps) *PreferenceReportingServer {
	consentRecordSync := customersv1.NewConsentRecordSyncServiceClient(deps.Conn)
	return &PreferenceReportingServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.customers.v1.PreferenceReportingService"),
		conn:                           deps.Conn,
		sessionNotificationClient:      identityv1.NewSessionNotificationServiceClient(deps.Conn),
		auditEventValidationClient:     auditv1.NewAuditEventValidationServiceClient(deps.Conn),
		concessionStandQueryClient:     concessionsv1.NewConcessionStandQueryServiceClient(deps.Conn),
		syncConsentRecordsFn:           consentRecordSync.SyncConsentRecords,
		priceOverrideWorkflowClient:    pricingv1.NewPriceOverrideWorkflowServiceClient(deps.Conn),
		searchQueryNotificationClient:  searchv1.NewSearchQueryNotificationServiceClient(deps.Conn),
		householdLifecycleClient:       customersv1.NewHouseholdLifecycleServiceClient(deps.Conn),
		consentRecordSyncClient:        customersv1.NewConsentRecordSyncServiceClient(deps.Conn),
		meteredSponsorCalculation:      meteredSponsorCalculation{SponsorCalculationServiceClient: sponsorshipsv1.NewSponsorCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		priceQuoteApprovalClient:       pricingv1.NewPriceQuoteApprovalServiceClient(deps.Conn),
		preferenceImportClient:         customersv1.NewPreferenceImportServiceClient(deps.Conn),
		subscriptionPlanApprovalClient: subscriptionsv1.NewSubscriptionPlanApprovalServiceClient(deps.Conn),
		accessPolicyQueryClient:        authorizationv1.NewAccessPolicyQueryServiceClient(deps.Conn),
		preferenceCalculationClient:    customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		consentRecordSyncPort:          newConsentRecordSyncPort(deps),
	}
}

// GeneratePreferenceReport generates a preference.
func (s *PreferenceReportingServer) GeneratePreferenceReport(ctx context.Context, req *customersv1.GeneratePreferenceReportRequest) (*customersv1.GeneratePreferenceReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "preference", req.GetPreferenceId(), "generate_preference_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate preference report", "tenant", req.GetContext().GetTenantId())
	batchGetAuditEventsRequest := &auditv1.BatchGetAuditEventsRequest{Context: req.GetContext()}
	batchGetAuditEventsReply := new(auditv1.BatchGetAuditEventsResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.audit.v1.AuditEventService/BatchGetAuditEvents", batchGetAuditEventsRequest, batchGetAuditEventsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventService/BatchGetAuditEvents")
	}
	return &customersv1.GeneratePreferenceReportResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: batchGetAuditEventsReply.GetResult().GetReferenceIds(),
			Status:       batchGetAuditEventsReply.GetResult().GetStatus(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchPreferenceReport returns a preference.
func (s *PreferenceReportingServer) FetchPreferenceReport(ctx context.Context, req *customersv1.FetchPreferenceReportRequest) (*customersv1.FetchPreferenceReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected FetchPreferenceReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "preference", req.GetPreferenceId(), "fetch_preference_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch preference report", "tenant", req.GetContext().GetTenantId())
	step := &preferenceReportingServerFetchPreferenceReportNotifySessionWatchersStep{request: &identityv1.NotifySessionWatchersRequest{
		Context: req.GetContext(),
		Details: &identityv1.NotifySessionWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}}
	if err := s.runFetchPreferenceReportNotifySessionWatchers(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/NotifySessionWatchers")
	}
	notifySessionWatchersReply := step.reply
	lookupUserRequest := &identityv1.LookupUserRequest{
		Context: req.GetContext(),
		UserId:  req.GetContext().GetActor().GetUserId(),
	}
	lookupUserReply := new(identityv1.LookupUserResponse)
	if err := grpcx.Invoke(ctx, s.conn, identityv1.UserQueryService_LookupUser_FullMethodName, lookupUserRequest, lookupUserReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/LookupUser")
	}
	return &customersv1.FetchPreferenceReportResponse{
		Result: &commonv1.OperationResult{
			Message: notifySessionWatchersReply.GetResult().GetMessage(),
			Details: notifySessionWatchersReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{
				CreatedAt: lookupUserReply.GetUser().GetAudit().GetCreatedAt(),
			},
			PreferenceId: req.GetPreferenceId(),
		},
	}, nil
}

// SchedulePreferenceReport schedules a preference.
func (s *PreferenceReportingServer) SchedulePreferenceReport(ctx context.Context, req *customersv1.SchedulePreferenceReportRequest) (*customersv1.SchedulePreferenceReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SchedulePreferenceReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "preference", req.GetPreferenceId(), "schedule_preference_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule preference report", "tenant", req.GetContext().GetTenantId())
	shareHouseholdReportRequest := &customersv1.ShareHouseholdReportRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
	}
	shareHouseholdReportReply := new(customersv1.ShareHouseholdReportResponse)
	if err := s.conn.Invoke(ctx, customersHouseholdReportingServiceShareHouseholdReportMethod, shareHouseholdReportRequest, shareHouseholdReportReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdReportingService/ShareHouseholdReport")
	}
	normalizeAuditEventReply, err := s.auditEventValidationClient.NormalizeAuditEvent(ctx, &auditv1.NormalizeAuditEventRequest{
		Context: req.GetContext(),
		AuditEvent: &auditv1.AuditEvent{
			Subject: &auditv1.AuditTrailRef{
				DisplayName: shareHouseholdReportReply.GetHousehold().GetManager().GetDisplayName(),
			},
			CorrelationId: shareHouseholdReportReply.GetResult().GetCorrelationId(),
		},
		Details: &auditv1.NormalizeAuditEventRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: shareHouseholdReportReply.GetHousehold().GetManager().GetEmail(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventValidationService/NormalizeAuditEvent")
	}
	countConcessionStandsRequest := &concessionsv1.CountConcessionStandsRequest{
		Context: req.GetContext(),
		Filter: &concessionsv1.CountConcessionStandsRequest_Filter{
			Region: shareHouseholdReportReply.GetHousehold().GetLocation().GetRegion(),
		},
	}
	countConcessionStandsReply, err := grpcx.Call(ctx, s.concessionStandQueryClient.CountConcessionStands, countConcessionStandsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.concessions.v1.ConcessionStandQueryService/CountConcessionStands")
	}
	_ = countConcessionStandsReply
	return &customersv1.SchedulePreferenceReportResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  shareHouseholdReportReply.GetResult().GetReferenceIds(),
			Message:       shareHouseholdReportReply.GetResult().GetMessage(),
			Details:       normalizeAuditEventReply.GetResult().GetDetails(),
			CorrelationId: normalizeAuditEventReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// StreamPreferenceMetrics streams the changes of preferences as they happen.
func (s *PreferenceReportingServer) StreamPreferenceMetrics(req *customersv1.StreamPreferenceMetricsRequest, stream grpc.ServerStreamingServer[customersv1.StreamPreferenceMetricsResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "stream preference metrics", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetFilter()).(*customersv1.StreamPreferenceMetricsRequest_Filter)
	snapshot, err := protojson.Marshal(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "encode snapshot")
	}
	syncConsentRecordsRequest := &customersv1.SyncConsentRecordsRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Measurements: []*customersv1.ConsentRecordMeasurement{&customersv1.ConsentRecordMeasurement{ObservedAt: cloned.GetCreatedAfter()}},
		},
		Details: &customersv1.SyncConsentRecordsRequest_Details{Payload: snapshot},
	}
	syncConsentRecordsReply, err := s.syncConsentRecordsFn(ctx, syncConsentRecordsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.customers.v1.ConsentRecordSyncService/SyncConsentRecords")
	}
	getUserVersionRequest := &identityv1.GetUserVersionRequest{
		Context: req.GetContext(),
		UserId:  req.GetContext().GetActor().GetUserId(),
	}
	getUserVersionReply := new(identityv1.GetUserVersionResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.identity.v1.UserHistoryService/GetUserVersion", getUserVersionRequest, getUserVersionReply); err != nil {
		return grpcx.Wrap(err, "acme.identity.v1.UserHistoryService/GetUserVersion")
	}
	resolveAccessPoliciesRequest := &authorizationv1.ResolveAccessPoliciesRequest{Context: req.GetContext()}
	resolveAccessPoliciesReply := new(authorizationv1.ResolveAccessPoliciesResponse)
	if err := grpcx.Invoke(ctx, s.conn, authorizationv1.AccessPolicyQueryService_ResolveAccessPolicies_FullMethodName, resolveAccessPoliciesRequest, resolveAccessPoliciesReply); err != nil {
		return grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyQueryService/ResolveAccessPolicies")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := preferenceReportingServerPausePriceOverrideWorkflowSteps[mode]
	pausePriceOverrideWorkflowRequest := &pricingv1.PausePriceOverrideWorkflowRequest{
		Context: req.GetContext(),
		PriceOverride: &pricingv1.PriceOverride{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: getUserVersionReply.GetUser().GetAudit().GetUpdatedAt(),
			},
		},
	}
	pausePriceOverrideWorkflowReply, err := run(s, ctx, pausePriceOverrideWorkflowRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideWorkflowService/PausePriceOverrideWorkflow")
	}
	if err := stream.Send(&customersv1.StreamPreferenceMetricsResponse{
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				DisplayName: syncConsentRecordsReply.GetConsentRecord().GetSubject().GetDisplayName(),
				Email:       req.GetSubscriber().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: getUserVersionReply.GetUser().GetAudit().GetUpdatedBy(),
			},
			Parameters: pausePriceOverrideWorkflowReply.GetPriceOverride().GetParameters(),
		},
		Result: &commonv1.OperationResult{
			Details: syncConsentRecordsReply.GetResult().GetDetails(),
			Message: resolveAccessPoliciesReply.GetResult().GetMessage(),
			Status:  resolveAccessPoliciesReply.GetResult().GetStatus(),
		},
	}); err != nil {
		return err
	}
	return nil
}

// ListPreferenceReports lists preferences a page at a time.
func (s *PreferenceReportingServer) ListPreferenceReports(ctx context.Context, req *customersv1.ListPreferenceReportsRequest) (*customersv1.ListPreferenceReportsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListPreferenceReports", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list preference reports", "tenant", req.GetContext().GetTenantId())
	listSearchQuerySubscriptionsReply, err := s.searchQueryNotificationClient.ListSearchQuerySubscriptions(ctx, &searchv1.ListSearchQuerySubscriptionsRequest{
		Context: req.GetContext(),
		Page:    &commonv1.PageRequest{PageToken: req.GetPage().GetPageToken()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/ListSearchQuerySubscriptions")
	}
	return &customersv1.ListPreferenceReportsResponse{
		PageInfo: &commonv1.PageInfo{
			NextPageToken: listSearchQuerySubscriptionsReply.GetPageInfo().GetNextPageToken(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SharePreferenceReport shares a preference.
func (s *PreferenceReportingServer) SharePreferenceReport(ctx context.Context, req *customersv1.SharePreferenceReportRequest) (*customersv1.SharePreferenceReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "preference", req.GetPreferenceId(), "share_preference_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share preference report", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*customersv1.SharePreferenceReportRequest_Details)
	scheduleHouseholdExpiryReply, err := s.householdLifecycleClient.ScheduleHouseholdExpiry(ctx, &customersv1.ScheduleHouseholdExpiryRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
				}},
			}},
		},
		Details: &customersv1.ScheduleHouseholdExpiryRequest_Details{
			Notify: &commonv1.ContactInfo{
				PreferredChannel: cloned.GetRecipient().GetPreferredChannel(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdLifecycleService/ScheduleHouseholdExpiry")
	}
	var envelope customersv1.CustomerRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	syncConsentRecordsRequest := &customersv1.SyncConsentRecordsRequest{}
	syncConsentRecordsRequest.Context = req.GetContext()
	syncConsentRecordsRequest.ConsentRecord = &customersv1.ConsentRecord{
		Subject: &customersv1.HouseholdRef{
			DisplayName: scheduleHouseholdExpiryReply.GetHousehold().GetManager().GetDisplayName(),
		},
		Attributes:    scheduleHouseholdExpiryReply.GetHousehold().GetAttributes(),
		CorrelationId: scheduleHouseholdExpiryReply.GetResult().GetCorrelationId(),
	}
	syncConsentRecordsRequest.ConsentRecordId = envelope.GetDisplayName()
	syncConsentRecordsReply, err := s.consentRecordSyncClient.SyncConsentRecords(ctx, syncConsentRecordsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordSyncService/SyncConsentRecords")
	}
	quoteSponsorRequest := &sponsorshipsv1.QuoteSponsorRequest{
		Context: req.GetContext(),
		Details: &sponsorshipsv1.QuoteSponsorRequest_Details{
			Destination: &commonv1.PostalAddress{
				Lines: scheduleHouseholdExpiryReply.GetHousehold().GetLocation().GetLines(),
			},
		},
		Sponsor: &sponsorshipsv1.Sponsor{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: scheduleHouseholdExpiryReply.GetHousehold().GetAudit().GetUpdatedAt(),
			},
		},
	}
	quoteSponsorReply, err := s.meteredSponsorCalculation.QuoteSponsor(ctx, quoteSponsorRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sponsorships.v1.SponsorCalculationService/QuoteSponsor")
	}
	return &customersv1.SharePreferenceReportResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  scheduleHouseholdExpiryReply.GetResult().GetReferenceIds(),
			CorrelationId: scheduleHouseholdExpiryReply.GetResult().GetCorrelationId(),
			Message:       syncConsentRecordsReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				Email: quoteSponsorReply.GetSponsor().GetContact().GetEmail(),
			},
			PreferenceId: req.GetPreferenceId(),
		},
	}, nil
}

// CountPreferenceTotals counts preferences by status.
func (s *PreferenceReportingServer) CountPreferenceTotals(ctx context.Context, req *customersv1.CountPreferenceTotalsRequest) (*customersv1.CountPreferenceTotalsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "count preference totals", "tenant", req.GetContext().GetTenantId())
	var holder struct{ Region string }
	if record := req.GetFilter(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Region"); err != nil {
			s.log.WarnContext(ctx, "copy region", "error", err)
		}
	}
	rejectPriceQuoteReply, err := s.priceQuoteApprovalClient.RejectPriceQuote(ctx, &pricingv1.RejectPriceQuoteRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			BillingAddress: &commonv1.PostalAddress{Region: holder.Region},
		},
		Details: &pricingv1.RejectPriceQuoteRequest_Details{
			Approver: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteApprovalService/RejectPriceQuote")
	}
	pageCache := cache.New[string, *commonv1.PageRequest](1)
	pageCache.Put(req.GetContext().GetTraceId(), req.GetPage())
	cachedPage, _ := pageCache.Get(req.GetContext().GetTraceId())
	listPreferenceImportsRequest := &customersv1.ListPreferenceImportsRequest{
		Context: req.GetContext(),
		Page: &commonv1.PageRequest{
			PageToken: req.GetPage().GetPageToken(),
			PageSize:  cachedPage.GetPageSize(),
		},
		Filter: &customersv1.ListPreferenceImportsRequest_Filter{
			Region: rejectPriceQuoteReply.GetPriceQuote().GetBillingAddress().GetRegion(),
			Labels: req.GetFilter().GetLabels(),
		},
	}
	var listPreferenceImportsReply *customersv1.ListPreferenceImportsResponse
	if err := pipeline.Run(ctx, "acme.customers.v1.PreferenceImportService/ListPreferenceImports", func(ctx context.Context) error {
		answer, err := s.preferenceImportClient.ListPreferenceImports(ctx, listPreferenceImportsRequest)
		listPreferenceImportsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceImportService/ListPreferenceImports")
	}
	return &customersv1.CountPreferenceTotalsResponse{
		Result: &commonv1.OperationResult{
			Details: rejectPriceQuoteReply.GetResult().GetDetails(),
			Message: listPreferenceImportsReply.GetResult().GetMessage(),
			Status:  listPreferenceImportsReply.GetResult().GetStatus(),
		},
	}, nil
}

// DeletePreferenceReport deletes a preference.
func (s *PreferenceReportingServer) DeletePreferenceReport(ctx context.Context, req *customersv1.DeletePreferenceReportRequest) (*customersv1.DeletePreferenceReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "preference", req.GetPreferenceId(), "delete_preference_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete preference report", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	approveSubscriptionPlanRequest := &subscriptionsv1.ApproveSubscriptionPlanRequest{}
	approveSubscriptionPlanRequest.Context = req.GetContext()
	approveSubscriptionPlanRequest.SubscriptionPlan = &subscriptionsv1.SubscriptionPlan{
		Action: &subscriptionsv1.SubscriptionPlan_Block{Block: &subscriptionsv1.SubscriptionPlanBlockAction{Reason: tenancy.Value(ctx, "reason")}},
		Audit:  &commonv1.AuditInfo{Etag: req.GetEtag()},
	}
	approveSubscriptionPlanRequest.Details = &subscriptionsv1.ApproveSubscriptionPlanRequest_Details{
		Approver: &identityv1.UserRef{UserId: cloned.GetActor().GetUserId()},
	}
	approveSubscriptionPlanReply, err := s.subscriptionPlanApprovalClient.ApproveSubscriptionPlan(ctx, approveSubscriptionPlanRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanApprovalService/ApproveSubscriptionPlan")
	}
	countAccessPoliciesRequest := &authorizationv1.CountAccessPoliciesRequest{}
	countAccessPoliciesRequest.Context = req.GetContext()
	countAccessPoliciesRequest.Filter = &authorizationv1.CountAccessPoliciesRequest_Filter{
		CreatedAfter: approveSubscriptionPlanReply.GetOutcome().GetEffectiveAt(),
		OwnerId:      approveSubscriptionPlanReply.GetSubscriptionPlan().GetSubscriptionPlanId(),
		Labels:       approveSubscriptionPlanReply.GetSubscriptionPlan().GetParameters(),
	}
	countAccessPoliciesReply, err := s.accessPolicyQueryClient.CountAccessPolicies(ctx, countAccessPoliciesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyQueryService/CountAccessPolicies")
	}
	recalculatePreferencesRequest := &customersv1.RecalculatePreferencesRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Action: &customersv1.Preference_Block{Block: &customersv1.PreferenceBlockAction{
				Reason: approveSubscriptionPlanReply.GetSubscriptionPlan().GetBlock().GetReason(),
			}},
			Owner: &identityv1.UserRef{
				UserId: approveSubscriptionPlanReply.GetSubscriptionPlan().GetOwner().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{
				Etag:      approveSubscriptionPlanReply.GetSubscriptionPlan().GetAudit().GetEtag(),
				CreatedBy: approveSubscriptionPlanReply.GetSubscriptionPlan().GetAudit().GetCreatedBy(),
			},
		},
	}
	recalculatePreferencesReply, err := grpcx.Call(ctx, s.preferenceCalculationClient.RecalculatePreferences, recalculatePreferencesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/RecalculatePreferences")
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	pullConsentRecordsRequest := &customersv1.PullConsentRecordsRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: approveSubscriptionPlanReply.GetSubscriptionPlan().GetOwner().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	pullConsentRecordsReply, err := s.consentRecordSyncPort.PullConsentRecords(ctx, pullConsentRecordsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordSyncService/PullConsentRecords")
	}
	_ = pullConsentRecordsReply
	return &customersv1.DeletePreferenceReportResponse{
		EffectiveAt: approveSubscriptionPlanReply.GetOutcome().GetEffectiveAt(),
		Result: &commonv1.OperationResult{
			Status:  approveSubscriptionPlanReply.GetOutcome().GetStatus(),
			Message: countAccessPoliciesReply.GetResult().GetMessage(),
			Details: recalculatePreferencesReply.GetResult().GetDetails(),
		},
	}, nil
}

// preferenceReportingServerFetchPreferenceReportNotifySessionWatchersStep carries the request of one step of
// FetchPreferenceReport and, once it ran, its reply.
type preferenceReportingServerFetchPreferenceReportNotifySessionWatchersStep struct {
	request *identityv1.NotifySessionWatchersRequest
	reply   *identityv1.NotifySessionWatchersResponse
}

func (s *PreferenceReportingServer) runFetchPreferenceReportNotifySessionWatchers(ctx context.Context, step *preferenceReportingServerFetchPreferenceReportNotifySessionWatchersStep) error {
	reply, err := s.sessionNotificationClient.NotifySessionWatchers(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// preferenceReportingServerPausePriceOverrideWorkflowSteps are the ways to run
// acme.pricing.v1.PriceOverrideWorkflowService/PausePriceOverrideWorkflow, by name.
var preferenceReportingServerPausePriceOverrideWorkflowSteps = map[string]func(*PreferenceReportingServer, context.Context, *pricingv1.PausePriceOverrideWorkflowRequest) (*pricingv1.PausePriceOverrideWorkflowResponse, error){
	"default": (*PreferenceReportingServer).pausePriceOverrideWorkflowStep,
	"audited": (*PreferenceReportingServer).pausePriceOverrideWorkflowAuditedStep,
}

func (s *PreferenceReportingServer) pausePriceOverrideWorkflowStep(ctx context.Context, request *pricingv1.PausePriceOverrideWorkflowRequest) (*pricingv1.PausePriceOverrideWorkflowResponse, error) {
	return s.priceOverrideWorkflowClient.PausePriceOverrideWorkflow(ctx, request)
}

func (s *PreferenceReportingServer) pausePriceOverrideWorkflowAuditedStep(ctx context.Context, request *pricingv1.PausePriceOverrideWorkflowRequest) (*pricingv1.PausePriceOverrideWorkflowResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.pricing.v1.PriceOverrideWorkflowService/PausePriceOverrideWorkflow")
	return s.priceOverrideWorkflowClient.PausePriceOverrideWorkflow(ctx, request)
}
