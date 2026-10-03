package orders

import (
	"context"
	"errors"
	"io"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	collectionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/collections/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// BackorderReportingServer implements `acme.orders.v1.BackorderReportingService`.
// Builds and schedules reports about backorders.
type BackorderReportingServer struct {
	ordersv1.UnsafeBackorderReportingServiceServer
	store                         *Store
	log                           *slog.Logger
	conn                          *grpc.ClientConn
	householdValidationClient     customersv1.HouseholdValidationServiceClient
	orderClient                   ordersv1.OrderServiceClient
	backorderSyncClient           ordersv1.BackorderSyncServiceClient
	meteredSearchIndexQuery       meteredSearchIndexQuery
	sessionQueryClient            identityv1.SessionQueryServiceClient
	orderAmendmentReportingClient ordersv1.OrderAmendmentReportingServiceClient
	customerValidationClient      customersv1.CustomerValidationServiceClient
	searchIndexQueryClient        searchv1.SearchIndexQueryServiceClient
	consentRecordValidationClient customersv1.ConsentRecordValidationServiceClient
	clients                       *registry.Registry
	summarizeSessionsFn           func(context.Context, *identityv1.SummarizeSessionsRequest, ...grpc.CallOption) (*identityv1.SummarizeSessionsResponse, error)
}

// NewBackorderReportingServer wires a BackorderReportingServer.
func NewBackorderReportingServer(deps platform.Deps) *BackorderReportingServer {
	sessionQuery := identityv1.NewSessionQueryServiceClient(deps.Conn)
	return &BackorderReportingServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.orders.v1.BackorderReportingService"),
		conn:                          deps.Conn,
		householdValidationClient:     customersv1.NewHouseholdValidationServiceClient(deps.Conn),
		orderClient:                   ordersv1.NewOrderServiceClient(deps.Conn),
		backorderSyncClient:           ordersv1.NewBackorderSyncServiceClient(deps.Conn),
		meteredSearchIndexQuery:       meteredSearchIndexQuery{SearchIndexQueryServiceClient: searchv1.NewSearchIndexQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		sessionQueryClient:            identityv1.NewSessionQueryServiceClient(deps.Conn),
		orderAmendmentReportingClient: ordersv1.NewOrderAmendmentReportingServiceClient(deps.Conn),
		customerValidationClient:      customersv1.NewCustomerValidationServiceClient(deps.Conn),
		searchIndexQueryClient:        searchv1.NewSearchIndexQueryServiceClient(deps.Conn),
		consentRecordValidationClient: customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		clients:                       deps.Clients,
		summarizeSessionsFn:           sessionQuery.SummarizeSessions,
	}
}

// GenerateBackorderReport generates a backorder.
func (s *BackorderReportingServer) GenerateBackorderReport(ctx context.Context, req *ordersv1.GenerateBackorderReportRequest) (*ordersv1.GenerateBackorderReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "backorder", req.GetBackorderId(), "generate_backorder_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate backorder report", "tenant", req.GetContext().GetTenantId())
	lintAccessPoliciesRequest := &authorizationv1.LintAccessPoliciesRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
			Action: &authorizationv1.AccessPolicy_Block{Block: &authorizationv1.AccessPolicyBlockAction{Reason: req.GetReason()}},
		},
	}
	lintAccessPoliciesReply, err := grpcx.NewClient(s.conn, authorizationv1.NewAccessPolicyValidationServiceClient).LintAccessPolicies(ctx, lintAccessPoliciesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/LintAccessPolicies")
	}
	scoreHouseholdRequest := &customersv1.ScoreHouseholdRequest{}
	scoreHouseholdRequest.Context = req.GetContext()
	scoreHouseholdRequest.Household = &customersv1.Household{
		Children: []*customersv1.Household{&customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Audit: &commonv1.AuditInfo{
					CreatedBy: lintAccessPoliciesReply.GetAccessPolicy().GetAudit().GetCreatedBy(),
				},
				Manager: &identityv1.UserRef{
					UserId: lintAccessPoliciesReply.GetAccessPolicy().GetOwner().GetUserId(),
				},
				Children: []*customersv1.Household{&customersv1.Household{Name: lintAccessPoliciesReply.GetAccessPolicy().GetName()}},
			}},
		}},
	}
	scoreHouseholdReply, err := s.householdValidationClient.ScoreHousehold(ctx, scoreHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/ScoreHousehold")
	}
	watchOrdersStream, err := s.orderClient.WatchOrders(ctx, &ordersv1.WatchOrdersRequest{
		Context: req.GetContext(),
		Filter: &ordersv1.WatchOrdersRequest_Filter{
			Region: scoreHouseholdReply.GetHousehold().GetLocation().GetRegion(),
		},
		Subscriber: &commonv1.ContactInfo{
			Email: scoreHouseholdReply.GetHousehold().GetManager().GetEmail(),
		},
		Since: lintAccessPoliciesReply.GetAccessPolicy().GetAudit().GetCreatedAt(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderService/WatchOrders")
	}
	for {
		received, err := watchOrdersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.orders.v1.OrderService/WatchOrders")
		}
		_ = received
	}
	listBackorderConflictsReply, err := s.backorderSyncClient.ListBackorderConflicts(ctx, &ordersv1.ListBackorderConflictsRequest{
		Context: req.GetContext(),
		Filter: &ordersv1.ListBackorderConflictsRequest_Filter{
			Region: scoreHouseholdReply.GetHousehold().GetLocation().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderSyncService/ListBackorderConflicts")
	}
	_ = listBackorderConflictsReply
	return &ordersv1.GenerateBackorderReportResponse{
		Result: &commonv1.OperationResult{
			Details: lintAccessPoliciesReply.GetResult().GetDetails(),
			Message: scoreHouseholdReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchBackorderReport returns a backorder.
func (s *BackorderReportingServer) FetchBackorderReport(ctx context.Context, req *ordersv1.FetchBackorderReportRequest) (*ordersv1.FetchBackorderReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "backorder", req.GetBackorderId(), "fetch_backorder_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch backorder report", "tenant", req.GetContext().GetTenantId())
	countSearchIndexesRequest := &searchv1.CountSearchIndexesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.CountSearchIndexesRequest_Filter{
			OwnerId:      req.GetBackorderId(),
			CreatedAfter: req.GetContext().GetDeadline(),
			Labels:       req.GetContext().GetBaggage(),
		},
	}
	countSearchIndexesReply, err := s.meteredSearchIndexQuery.CountSearchIndexes(ctx, countSearchIndexesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexQueryService/CountSearchIndexes")
	}
	countSessionsReply, err := s.sessionQueryClient.CountSessions(ctx, &identityv1.CountSessionsRequest{
		Context: req.GetContext(),
		Filter:  &identityv1.CountSessionsRequest_Filter{Labels: countSearchIndexesReply.GetResult().GetDetails()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/CountSessions")
	}
	return &ordersv1.FetchBackorderReportResponse{
		Result: &commonv1.OperationResult{
			Message:       countSearchIndexesReply.GetResult().GetMessage(),
			CorrelationId: countSearchIndexesReply.GetResult().GetCorrelationId(),
			ReferenceIds:  countSessionsReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		Backorder: &ordersv1.Backorder{BackorderId: req.GetBackorderId()},
	}, nil
}

// StreamBackorderMetrics streams the changes of backorders as they happen.
func (s *BackorderReportingServer) StreamBackorderMetrics(req *ordersv1.StreamBackorderMetricsRequest, stream grpc.ServerStreamingServer[ordersv1.StreamBackorderMetricsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		return err
	}
	s.log.InfoContext(ctx, "stream backorder metrics", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetSubscriber())
	if err != nil {
		return grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.ContactInfo
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return err
	}
	previewPreferenceRequest := &customersv1.PreviewPreferenceRequest{
		Context: req.GetContext(),
		Details: &customersv1.PreviewPreferenceRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:             req.GetSubscriber().GetName(),
				Phone:            (&decoded).GetPhone(),
				PreferredChannel: req.GetSubscriber().GetPreferredChannel(),
			},
		},
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{Email: req.GetSubscriber().GetEmail()},
		},
	}
	previewPreferenceReply := new(customersv1.PreviewPreferenceResponse)
	if err := grpcx.Invoke(ctx, s.conn, customersv1.PreferenceValidationService_PreviewPreference_FullMethodName, previewPreferenceRequest, previewPreferenceReply); err != nil {
		return grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/PreviewPreference")
	}
	var holder struct{ Query string }
	if record := req.GetFilter(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Query"); err != nil {
			s.log.WarnContext(ctx, "copy query", "error", err)
		}
	}
	browseCollectionCasesRequest := &collectionsv1.BrowseCollectionCasesRequest{
		Context: req.GetContext(),
		Filter: &collectionsv1.BrowseCollectionCasesRequest_Filter{
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			Query:        holder.Query,
		},
	}
	browseCollectionCasesReply, err := browseCollectionCasesCall(ctx, browseCollectionCasesRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.collections.v1.CollectionCaseQueryService/BrowseCollectionCases")
	}
	countOrderAmendmentTotalsRequest := buildCountOrderAmendmentTotalsRequestForStreamBackorderMetrics(req)
	countOrderAmendmentTotalsReply, err := s.orderAmendmentReportingClient.CountOrderAmendmentTotals(ctx, countOrderAmendmentTotalsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentReportingService/CountOrderAmendmentTotals")
	}
	_ = countOrderAmendmentTotalsReply
	if err := stream.Send(&ordersv1.StreamBackorderMetricsResponse{
		Result: &commonv1.OperationResult{
			Message: previewPreferenceReply.GetResult().GetMessage(),
			Details: browseCollectionCasesReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Backorder: &ordersv1.Backorder{
			Requester: &commonv1.ContactInfo{
				Email: req.GetSubscriber().GetEmail(),
				Phone: req.GetSubscriber().GetPhone(),
			},
		},
	}); err != nil {
		return err
	}
	return nil
}

// ListBackorderReports lists backorders a page at a time.
func (s *BackorderReportingServer) ListBackorderReports(ctx context.Context, req *ordersv1.ListBackorderReportsRequest) (*ordersv1.ListBackorderReportsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list backorder reports", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "region", req.GetFilter().GetRegion())
	lintCustomersRequest := newLintCustomersRequest(withLintCustomersContext(req.GetContext()), withLintCustomersCustomer(&customersv1.Customer{
		PrimaryAddress: &commonv1.PostalAddress{Region: tenancy.Value(ctx, "region")},
	}))
	lintCustomersReply, err := s.customerValidationClient.LintCustomers(ctx, lintCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/LintCustomers")
	}
	resolveSearchIndexesRequest := &searchv1.ResolveSearchIndexesRequest{}
	resolveSearchIndexesRequest.Context = req.GetContext()
	resolveSearchIndexesReply, err := s.searchIndexQueryClient.ResolveSearchIndexes(ctx, resolveSearchIndexesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexQueryService/ResolveSearchIndexes")
	}
	return &ordersv1.ListBackorderReportsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  lintCustomersReply.GetResult().GetReferenceIds(),
			CorrelationId: resolveSearchIndexesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// ShareBackorderReport shares a backorder.
func (s *BackorderReportingServer) ShareBackorderReport(ctx context.Context, req *ordersv1.ShareBackorderReportRequest) (*ordersv1.ShareBackorderReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "backorder", req.GetBackorderId(), "share_backorder_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share backorder report", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded ordersv1.ShareBackorderReportRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	cloned := proto.Clone(req.GetDetails()).(*ordersv1.ShareBackorderReportRequest_Details)
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	normalizeConsentRecordRequest := &customersv1.NormalizeConsentRecordRequest{
		Context: req.GetContext(),
		Details: &customersv1.NormalizeConsentRecordRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:             (&decoded).GetRecipient().GetName(),
				PreferredChannel: req.GetDetails().GetRecipient().GetPreferredChannel(),
				Phone:            cloned.GetRecipient().GetPhone(),
			},
		},
		ConsentRecord: &customersv1.ConsentRecord{Detail: packed},
	}
	type normalizeConsentRecordResult struct {
		reply *customersv1.NormalizeConsentRecordResponse
		err   error
	}
	normalizeConsentRecordResults := make(chan normalizeConsentRecordResult, 1)
	go func() {
		reply, err := s.consentRecordValidationClient.NormalizeConsentRecord(ctx, normalizeConsentRecordRequest)
		normalizeConsentRecordResults <- normalizeConsentRecordResult{reply, err}
	}()
	var normalizeConsentRecordReply *customersv1.NormalizeConsentRecordResponse
	select {
	case result := <-normalizeConsentRecordResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.ConsentRecordValidationService/NormalizeConsentRecord")
		}
		normalizeConsentRecordReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	backorderSync, err := registry.Lookup[ordersv1.BackorderSyncServiceClient](s.clients, "acme.orders.v1.BackorderSyncService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	var envelope ordersv1.OrderRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	syncBackordersRequest := &ordersv1.SyncBackordersRequest{
		Context: req.GetContext(),
		Backorder: &ordersv1.Backorder{
			Requester: &commonv1.ContactInfo{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Details:     &ordersv1.SyncBackordersRequest_Details{Payload: snapshot},
		BackorderId: envelope.GetDisplayName(),
	}
	syncBackordersReply, err := backorderSync.SyncBackorders(ctx, syncBackordersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderSyncService/SyncBackorders")
	}
	var envelope2 ordersv1.OrderRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	resumeMerchandisingRuleWorkflowRequest := &merchandisingv1.ResumeMerchandisingRuleWorkflowRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{
				UserId: syncBackordersReply.GetBackorder().GetAssignee().GetUserId(),
			},
		},
		Details: &merchandisingv1.ResumeMerchandisingRuleWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetRecipient().GetPreferredChannel(),
				Email:            syncBackordersReply.GetBackorder().GetAssignee().GetEmail(),
			},
		},
		MerchandisingRuleId: envelope2.GetDisplayName(),
	}
	resumeMerchandisingRuleWorkflowReply := new(merchandisingv1.ResumeMerchandisingRuleWorkflowResponse)
	if err := grpc.Invoke(ctx, "/acme.merchandising.v1.MerchandisingRuleWorkflowService/ResumeMerchandisingRuleWorkflow", resumeMerchandisingRuleWorkflowRequest, resumeMerchandisingRuleWorkflowReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleWorkflowService/ResumeMerchandisingRuleWorkflow")
	}
	return &ordersv1.ShareBackorderReportResponse{
		Outcome: &ordersv1.ShareBackorderReportResponse_Outcome{
			NextSteps: normalizeConsentRecordReply.GetOutcome().GetNextSteps(),
		},
		Backorder: &ordersv1.Backorder{
			Assignee: &identityv1.UserRef{
				DisplayName: normalizeConsentRecordReply.GetConsentRecord().GetSubject().GetDisplayName(),
				Email:       resumeMerchandisingRuleWorkflowReply.GetMerchandisingRule().GetOwner().GetEmail(),
			},
			Sla: &ordersv1.BackorderSla{
				PolicyId: syncBackordersReply.GetBackorder().GetSla().GetPolicyId(),
			},
			BackorderId: req.GetBackorderId(),
			Requester: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetRecipient().GetPreferredChannel(),
			},
		},
		Result: &commonv1.OperationResult{
			Details: syncBackordersReply.GetResult().GetDetails(),
			Message: resumeMerchandisingRuleWorkflowReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// CountBackorderTotals counts backorders by status.
func (s *BackorderReportingServer) CountBackorderTotals(ctx context.Context, req *ordersv1.CountBackorderTotalsRequest) (*ordersv1.CountBackorderTotalsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "count backorder totals", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	scheduleOrderAmendmentReportReply, err := s.orderAmendmentReportingClient.ScheduleOrderAmendmentReport(ctx, &ordersv1.ScheduleOrderAmendmentReportRequest{
		Context:     req.GetContext(),
		Details:     &ordersv1.ScheduleOrderAmendmentReportRequest_Details{Filters: labels},
		Annotations: req.GetContext().GetBaggage(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentReportingService/ScheduleOrderAmendmentReport")
	}
	return &ordersv1.CountBackorderTotalsResponse{
		Result: &commonv1.OperationResult{
			Message:      scheduleOrderAmendmentReportReply.GetResult().GetMessage(),
			ReferenceIds: scheduleOrderAmendmentReportReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// DeleteBackorderReport deletes a backorder.
func (s *BackorderReportingServer) DeleteBackorderReport(ctx context.Context, req *ordersv1.DeleteBackorderReportRequest) (*ordersv1.DeleteBackorderReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected DeleteBackorderReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "backorder", req.GetBackorderId(), "delete_backorder_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete backorder report", "tenant", req.GetContext().GetTenantId())
	orderAmendmentValidation, ok := s.clients.Get("acme.orders.v1.OrderAmendmentValidationService").(ordersv1.OrderAmendmentValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.orders.v1.OrderAmendmentValidationService")
	}
	explainOrderAmendmentDecisionRequest := &ordersv1.ExplainOrderAmendmentDecisionRequest{
		Context: req.GetContext(),
		OrderAmendment: &ordersv1.OrderAmendment{
			Counterparty: &ordersv1.BackorderRef{BackorderId: req.GetBackorderId()},
			Audit:        &commonv1.AuditInfo{Etag: req.GetEtag()},
		},
	}
	explainOrderAmendmentDecisionReply, err := orderAmendmentValidation.ExplainOrderAmendmentDecision(ctx, explainOrderAmendmentDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentValidationService/ExplainOrderAmendmentDecision")
	}
	summarizeSessionsRequest := &identityv1.SummarizeSessionsRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
		Details: &identityv1.SummarizeSessionsRequest_Details{
			Filters: explainOrderAmendmentDecisionReply.GetOrderAmendment().GetMetadata(),
		},
		SessionId: explainOrderAmendmentDecisionReply.GetOrderAmendment().GetCounterparty().GetBackorderId(),
	}
	summarizeSessionsReply, err := s.summarizeSessionsFn(ctx, summarizeSessionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/SummarizeSessions")
	}
	return &ordersv1.DeleteBackorderReportResponse{
		Result: &commonv1.OperationResult{
			Details:       explainOrderAmendmentDecisionReply.GetResult().GetDetails(),
			CorrelationId: explainOrderAmendmentDecisionReply.GetResult().GetCorrelationId(),
			Status:        summarizeSessionsReply.GetResult().GetStatus(),
		},
	}, nil
}
