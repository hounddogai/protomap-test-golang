package customers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	performersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/performers/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// HouseholdApprovalServer implements `acme.customers.v1.HouseholdApprovalService`.
// Routes households through approval, with escalation and delegation.
type HouseholdApprovalServer struct {
	customersv1.UnsafeHouseholdApprovalServiceServer
	store                          *Store
	log                            *slog.Logger
	sellerListingWorkflowClient    sellersv1.SellerListingWorkflowServiceClient
	meteredPickListWorkflow        meteredPickListWorkflow
	householdLifecycleClient       customersv1.HouseholdLifecycleServiceClient
	customerSegmentReportingClient customersv1.CustomerSegmentReportingServiceClient
	bus                            *eventbus.Bus
	clients                        *registry.Registry
	searchIndexNotificationClient  searchv1.SearchIndexNotificationServiceClient
	tenantAdminClient              identityv1.TenantAdminServiceClient
	conn                           *grpc.ClientConn
	paymentInstrumentSyncClient    paymentsv1.PaymentInstrumentSyncServiceClient
	resolveAuditEventsFn           func(context.Context, *auditv1.ResolveAuditEventsRequest, ...grpc.CallOption) (*auditv1.ResolveAuditEventsResponse, error)
	promotionApprovalClient        promotionsv1.PromotionApprovalServiceClient
	notificationClient             notificationsv1.NotificationServiceClient
	userValidationClient           identityv1.UserValidationServiceClient
	userValidationClientFallback   identityv1.UserValidationServiceClient
}

// NewHouseholdApprovalServer wires a HouseholdApprovalServer.
func NewHouseholdApprovalServer(deps platform.Deps) *HouseholdApprovalServer {
	auditEventQuery := auditv1.NewAuditEventQueryServiceClient(deps.Conn)
	return &HouseholdApprovalServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.customers.v1.HouseholdApprovalService"),
		sellerListingWorkflowClient:    sellersv1.NewSellerListingWorkflowServiceClient(deps.Conn),
		meteredPickListWorkflow:        meteredPickListWorkflow{PickListWorkflowServiceClient: warehousingv1.NewPickListWorkflowServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		householdLifecycleClient:       customersv1.NewHouseholdLifecycleServiceClient(deps.Conn),
		customerSegmentReportingClient: customersv1.NewCustomerSegmentReportingServiceClient(deps.Conn),
		bus:                            deps.Bus,
		clients:                        deps.Clients,
		searchIndexNotificationClient:  searchv1.NewSearchIndexNotificationServiceClient(deps.Conn),
		tenantAdminClient:              identityv1.NewTenantAdminServiceClient(deps.Conn),
		conn:                           deps.Conn,
		paymentInstrumentSyncClient:    paymentsv1.NewPaymentInstrumentSyncServiceClient(deps.Conn),
		resolveAuditEventsFn:           auditEventQuery.ResolveAuditEvents,
		promotionApprovalClient:        promotionsv1.NewPromotionApprovalServiceClient(deps.Conn),
		notificationClient:             notificationsv1.NewNotificationServiceClient(deps.Conn),
		userValidationClient:           identityv1.NewUserValidationServiceClient(deps.Conn),
		userValidationClientFallback:   identityv1.NewUserValidationServiceClient(deps.Conn),
	}
}

// SubmitHousehold submits a household.
func (s *HouseholdApprovalServer) SubmitHousehold(ctx context.Context, req *customersv1.SubmitHouseholdRequest) (*customersv1.SubmitHouseholdResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "submit household", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := householdApprovalServerCancelSellerListingWorkflowSteps[mode]
	names := collectHouseholdNames(req.GetHousehold(), nil)
	cancelSellerListingWorkflowRequest := &sellersv1.CancelSellerListingWorkflowRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Owner:        &identityv1.UserRef{Email: req.GetHousehold().GetManager().GetEmail()},
			CategoryPath: names,
		},
	}
	cancelSellerListingWorkflowReply, err := run(s, ctx, cancelSellerListingWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingWorkflowService/CancelSellerListingWorkflow")
	}
	source := req.GetHousehold().GetLocation().ProtoReflect()
	recipient := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("recipient"))).String()
	resumePickListWorkflowRequest := &warehousingv1.ResumePickListWorkflowRequest{
		Context: req.GetContext(),
		PickList: &warehousingv1.PickList{
			Counterparty: &customersv1.CustomerRef{Email: req.GetDetails().GetApprover().GetEmail()},
			BillingAddress: &commonv1.PostalAddress{
				Recipient: recipient,
				Lines:     req.GetHousehold().GetLocation().GetLines(),
			},
		},
	}
	resumePickListWorkflowReply, err := s.meteredPickListWorkflow.ResumePickListWorkflow(ctx, resumePickListWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListWorkflowService/ResumePickListWorkflow")
	}
	terminateHouseholdReply, err := s.householdLifecycleClient.TerminateHousehold(ctx, &customersv1.TerminateHouseholdRequest{
		Context:     req.GetContext(),
		HouseholdId: req.GetHousehold().GetHouseholdId(),
		Details: &customersv1.TerminateHouseholdRequest_Details{
			NewOwner: &identityv1.UserRef{
				UserId:      cancelSellerListingWorkflowReply.GetSellerListing().GetOwner().GetUserId(),
				DisplayName: cancelSellerListingWorkflowReply.GetSellerListing().GetOwner().GetDisplayName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdLifecycleService/TerminateHousehold")
	}
	mode2 := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode2 = "audited"
	}
	run2 := householdApprovalServerCountCustomerSegmentTotalsSteps[mode2]
	countCustomerSegmentTotalsRequest := &customersv1.CountCustomerSegmentTotalsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.CountCustomerSegmentTotalsRequest_Filter{
			Region: terminateHouseholdReply.GetHousehold().GetLocation().GetRegion(),
		},
	}
	countCustomerSegmentTotalsReply, err := run2(s, ctx, countCustomerSegmentTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentReportingService/CountCustomerSegmentTotals")
	}
	return &customersv1.SubmitHouseholdResponse{
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit: &commonv1.AuditInfo{
						CreatedBy: cancelSellerListingWorkflowReply.GetSellerListing().GetAudit().GetCreatedBy(),
					},
				}},
				Audit: &commonv1.AuditInfo{
					CreatedBy: resumePickListWorkflowReply.GetPickList().GetAudit().GetCreatedBy(),
				},
				Location: &commonv1.PostalAddress{
					Lines: terminateHouseholdReply.GetHousehold().GetLocation().GetLines(),
				},
				Manager: terminateHouseholdReply.GetHousehold().GetManager(),
			}},
			HouseholdId: req.GetHouseholdId(),
		},
		Outcome: &customersv1.SubmitHouseholdResponse_Outcome{
			Message:     resumePickListWorkflowReply.GetOutcome().GetMessage(),
			Status:      countCustomerSegmentTotalsReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: countCustomerSegmentTotalsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// ApproveHousehold approves a household.
func (s *HouseholdApprovalServer) ApproveHousehold(ctx context.Context, req *customersv1.ApproveHouseholdRequest) (*customersv1.ApproveHouseholdResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "approve household", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "customers.approve_household.count_audit_events",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"filter.region":     req.GetHousehold().GetLocation().GetRegion(),
			"filter.owner_id":   req.GetHousehold().GetManager().GetUserId(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	searchQueryExport, ok := s.clients.Get("acme.search.v1.SearchQueryExportService").(searchv1.SearchQueryExportServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.search.v1.SearchQueryExportService")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	scheduleSearchQueryExportRequest := &searchv1.ScheduleSearchQueryExportRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      req.GetReason(),
	}
	scheduleSearchQueryExportReply, err := searchQueryExport.ScheduleSearchQueryExport(ctx, scheduleSearchQueryExportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryExportService/ScheduleSearchQueryExport")
	}
	return &customersv1.ApproveHouseholdResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: scheduleSearchQueryExportReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Household: &customersv1.Household{HouseholdId: req.GetHouseholdId()},
		Outcome:   &customersv1.ApproveHouseholdResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// RejectHousehold rejects a household.
func (s *HouseholdApprovalServer) RejectHousehold(ctx context.Context, req *customersv1.RejectHouseholdRequest) (*customersv1.RejectHouseholdResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "reject household", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	listPickListWorkflowsRequest := &warehousingv1.ListPickListWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &warehousingv1.ListPickListWorkflowsRequest_Filter{
			Region:       req.GetHousehold().GetLocation().GetRegion(),
			CreatedAfter: req.GetHousehold().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetDetails().GetApprover().GetUserId(),
			Labels:       annotations,
		},
	}
	listPickListWorkflowsReply, err := s.meteredPickListWorkflow.ListPickListWorkflows(ctx, listPickListWorkflowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListWorkflowService/ListPickListWorkflows")
	}
	streamSearchIndexNoticesStream, err := s.searchIndexNotificationClient.StreamSearchIndexNotices(ctx, &searchv1.StreamSearchIndexNoticesRequest{
		Context: req.GetContext(),
		Filter:  &searchv1.StreamSearchIndexNoticesRequest_Filter{Region: req.GetHousehold().GetLocation().GetRegion()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/StreamSearchIndexNotices")
	}
	for {
		received, err := streamSearchIndexNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/StreamSearchIndexNotices")
		}
		_ = received
	}
	return &customersv1.RejectHouseholdResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: listPickListWorkflowsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Household: &customersv1.Household{
			HouseholdId: req.GetHouseholdId(),
			Location:    req.GetHousehold().GetLocation(),
		},
	}, nil
}

// EscalateHousehold escalates a household.
func (s *HouseholdApprovalServer) EscalateHousehold(ctx context.Context, req *customersv1.EscalateHouseholdRequest) (*customersv1.EscalateHouseholdResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EscalateHousehold", "error", err)
		return nil, err
	}
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "escalate household", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetHousehold())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded customersv1.Household
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	archiveTenantRequest := &identityv1.ArchiveTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Details: &identityv1.ArchiveTenantRequest_Details{
			NewOwner: &identityv1.UserRef{
				Email:  req.GetDetails().GetApprover().GetEmail(),
				UserId: (&decoded).GetManager().GetUserId(),
			},
		},
	}
	type archiveTenantResult struct {
		reply *identityv1.ArchiveTenantResponse
		err   error
	}
	archiveTenantResults := make(chan archiveTenantResult, 1)
	go func() {
		reply, err := s.tenantAdminClient.ArchiveTenant(ctx, archiveTenantRequest)
		archiveTenantResults <- archiveTenantResult{reply, err}
	}()
	var archiveTenantReply *identityv1.ArchiveTenantResponse
	select {
	case result := <-archiveTenantResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.identity.v1.TenantAdminService/ArchiveTenant")
		}
		archiveTenantReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &customersv1.EscalateHouseholdResponse{
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Location: &commonv1.PostalAddress{
						Region: archiveTenantReply.GetTenant().GetLocation().GetRegion(),
					},
				}},
			}},
			HouseholdId: req.GetHouseholdId(),
			Status:      req.GetHousehold().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// DelegateHouseholdApproval delegates a household.
func (s *HouseholdApprovalServer) DelegateHouseholdApproval(ctx context.Context, req *customersv1.DelegateHouseholdApprovalRequest) (*customersv1.DelegateHouseholdApprovalResponse, error) {
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "delegate household approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	names := collectHouseholdNames(req.GetHousehold(), nil)
	summarizePerformersRequest := &performersv1.SummarizePerformersRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: annotations,
		Details: &performersv1.SummarizePerformersRequest_Details{
			Filters:         req.GetHousehold().GetAttributes(),
			RecipientEmails: names,
		},
	}
	summarizePerformersReply := new(performersv1.SummarizePerformersResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", performersPerformerQueryServiceName, "SummarizePerformers"), summarizePerformersRequest, summarizePerformersReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.performers.v1.PerformerQueryService/SummarizePerformers")
	}
	return &customersv1.DelegateHouseholdApprovalResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: summarizePerformersReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &customersv1.DelegateHouseholdApprovalResponse_Outcome{Status: summarizePerformersReply.GetResult().GetStatus()},
		Household: &customersv1.Household{
			HouseholdId: req.GetHouseholdId(),
			Depth:       req.GetHousehold().GetDepth(),
		},
	}, nil
}

// RecallHousehold recalls a household.
func (s *HouseholdApprovalServer) RecallHousehold(ctx context.Context, req *customersv1.RecallHouseholdRequest) (*customersv1.RecallHouseholdResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "recall household", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	snapshot, err := protojson.Marshal(req.GetHousehold())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pullPaymentInstrumentsRequest := &paymentsv1.PullPaymentInstrumentsRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetHousehold().GetAudit().GetUpdatedBy(),
				UpdatedAt: req.GetHousehold().GetAudit().GetUpdatedAt(),
			},
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
		Details: &paymentsv1.PullPaymentInstrumentsRequest_Details{Payload: snapshot},
	}
	pullPaymentInstrumentsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*paymentsv1.PullPaymentInstrumentsResponse, error) {
		return s.paymentInstrumentSyncClient.PullPaymentInstruments(ctx, pullPaymentInstrumentsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentSyncService/PullPaymentInstruments")
	}
	names := collectHouseholdNames(req.GetHousehold(), nil)
	resolveAuditEventsRequest := &auditv1.ResolveAuditEventsRequest{
		Context:       req.GetContext(),
		AuditEventIds: names,
	}
	resolveAuditEventsReply, err := s.resolveAuditEventsFn(ctx, resolveAuditEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventQueryService/ResolveAuditEvents")
	}
	signalReviewModerationWorkflowRequest := &reviewsv1.SignalReviewModerationWorkflowRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Assignee: &identityv1.UserRef{
				Email: pullPaymentInstrumentsReply.GetPaymentInstrument().GetHolder().GetEmail(),
			},
			Comments: []*reviewsv1.ReviewModerationComment{&reviewsv1.ReviewModerationComment{
				CreatedAt: pullPaymentInstrumentsReply.GetPaymentInstrument().GetAudit().GetCreatedAt(),
			}},
			Requester: &commonv1.ContactInfo{
				Email: pullPaymentInstrumentsReply.GetPaymentInstrument().GetContact().GetEmail(),
			},
		},
	}
	signalReviewModerationWorkflowReply := new(reviewsv1.SignalReviewModerationWorkflowResponse)
	if err := grpc.Invoke(ctx, "/acme.reviews.v1.ReviewModerationWorkflowService/SignalReviewModerationWorkflow", signalReviewModerationWorkflowRequest, signalReviewModerationWorkflowReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationWorkflowService/SignalReviewModerationWorkflow")
	}
	names2 := collectHouseholdNames(req.GetHousehold(), nil)
	simulatePreferenceRequest := &customersv1.SimulatePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner:  &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
			Scopes: names2,
		},
		Details: &customersv1.SimulatePreferenceRequest_Details{
			Destination: &commonv1.PostalAddress{Lines: req.GetHousehold().GetLocation().GetLines()},
		},
	}
	simulatePreferenceReply := new(customersv1.SimulatePreferenceResponse)
	if err := s.conn.Invoke(ctx, customersPreferenceCalculationServiceSimulatePreferenceMethod, simulatePreferenceRequest, simulatePreferenceReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/SimulatePreference")
	}
	return &customersv1.RecallHouseholdResponse{
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Manager: &identityv1.UserRef{
					Email: pullPaymentInstrumentsReply.GetPaymentInstrument().GetHolder().GetEmail(),
				},
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{
						Email: signalReviewModerationWorkflowReply.GetReviewModeration().GetCustomer().GetEmail(),
					},
				}},
				Name: simulatePreferenceReply.GetPreference().GetName(),
			}},
			HouseholdId: req.GetHouseholdId(),
			Attributes:  req.GetHousehold().GetAttributes(),
		},
		Outcome: &customersv1.RecallHouseholdResponse_Outcome{
			NextSteps: pullPaymentInstrumentsReply.GetOutcome().GetNextSteps(),
			Status:    resolveAuditEventsReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Details: simulatePreferenceReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// GetHouseholdApprovalStatus returns a household.
func (s *HouseholdApprovalServer) GetHouseholdApprovalStatus(ctx context.Context, req *customersv1.GetHouseholdApprovalStatusRequest) (*customersv1.GetHouseholdApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "household", req.GetHouseholdId(), "get_household_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get household approval status", "tenant", req.GetContext().GetTenantId())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "customers.get_household_approval_status.advance_collection_workflow",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"collection.children.manager.user_id": req.GetContext().GetActor().GetUserId(),
			"context.tenant_id":                   req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	rejectPromotionRequest := &promotionsv1.RejectPromotionRequest{}
	rejectPromotionRequest.Context = req.GetContext()
	rejectPromotionReply, err := s.promotionApprovalClient.RejectPromotion(ctx, rejectPromotionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionApprovalService/RejectPromotion")
	}
	return &customersv1.GetHouseholdApprovalStatusResponse{
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Manager: &identityv1.UserRef{
					Email: rejectPromotionReply.GetPromotion().GetOwner().GetEmail(),
				},
			}},
			HouseholdId: req.GetHouseholdId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentHouseholdApproval comments a household.
func (s *HouseholdApprovalServer) CommentHouseholdApproval(ctx context.Context, req *customersv1.CommentHouseholdApprovalRequest) (*customersv1.CommentHouseholdApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CommentHouseholdApproval", "error", err)
		return nil, err
	}
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "comment household approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	names := collectHouseholdNames(req.GetHousehold(), nil)
	step := &householdApprovalServerCommentHouseholdApprovalBatchGetNotificationsStep{request: &notificationsv1.BatchGetNotificationsRequest{
		Context:         req.GetContext(),
		NotificationIds: names,
	}}
	if err := s.runCommentHouseholdApprovalBatchGetNotifications(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationService/BatchGetNotifications")
	}
	batchGetNotificationsReply := step.reply
	routes := map[string]func(context.Context, *identityv1.NormalizeUserRequest, ...grpc.CallOption) (*identityv1.NormalizeUserResponse, error){
		"primary":  s.userValidationClient.NormalizeUser,
		"fallback": s.userValidationClientFallback.NormalizeUser,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	ctx = tenancy.With(ctx, "country_code", req.GetHousehold().GetLocation().GetCountryCode())
	names2 := collectHouseholdNames(req.GetHousehold(), nil)
	normalizeUserRequest := &identityv1.NormalizeUserRequest{
		Context: req.GetContext(),
		Details: &identityv1.NormalizeUserRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
		User: &identityv1.User{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetHousehold().GetAudit().GetEtag(),
				UpdatedAt: req.GetHousehold().GetAudit().GetUpdatedAt(),
			},
			PrimaryAddress: &commonv1.PostalAddress{
				CountryCode: tenancy.Value(ctx, "country_code"),
				Lines:       names2,
			},
		},
	}
	normalizeUserReply, err := route(ctx, normalizeUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/NormalizeUser")
	}
	return &customersv1.CommentHouseholdApprovalResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: batchGetNotificationsReply.GetResult().GetCorrelationId(),
			Message:       batchGetNotificationsReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		Household: &customersv1.Household{
			Location: &commonv1.PostalAddress{
				Locality: normalizeUserReply.GetUser().GetPrimaryAddress().GetLocality(),
			},
			HouseholdId: req.GetHouseholdId(),
			Attributes:  req.GetHousehold().GetAttributes(),
		},
	}, nil
}

// householdApprovalServerCancelSellerListingWorkflowSteps are the ways to run
// acme.sellers.v1.SellerListingWorkflowService/CancelSellerListingWorkflow, by name.
var householdApprovalServerCancelSellerListingWorkflowSteps = map[string]func(*HouseholdApprovalServer, context.Context, *sellersv1.CancelSellerListingWorkflowRequest) (*sellersv1.CancelSellerListingWorkflowResponse, error){
	"default": (*HouseholdApprovalServer).cancelSellerListingWorkflowStep,
	"audited": (*HouseholdApprovalServer).cancelSellerListingWorkflowAuditedStep,
}

func (s *HouseholdApprovalServer) cancelSellerListingWorkflowStep(ctx context.Context, request *sellersv1.CancelSellerListingWorkflowRequest) (*sellersv1.CancelSellerListingWorkflowResponse, error) {
	return s.sellerListingWorkflowClient.CancelSellerListingWorkflow(ctx, request)
}

func (s *HouseholdApprovalServer) cancelSellerListingWorkflowAuditedStep(ctx context.Context, request *sellersv1.CancelSellerListingWorkflowRequest) (*sellersv1.CancelSellerListingWorkflowResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.sellers.v1.SellerListingWorkflowService/CancelSellerListingWorkflow")
	return s.sellerListingWorkflowClient.CancelSellerListingWorkflow(ctx, request)
}

// householdApprovalServerCountCustomerSegmentTotalsSteps are the ways to run
// acme.customers.v1.CustomerSegmentReportingService/CountCustomerSegmentTotals, by name.
var householdApprovalServerCountCustomerSegmentTotalsSteps = map[string]func(*HouseholdApprovalServer, context.Context, *customersv1.CountCustomerSegmentTotalsRequest) (*customersv1.CountCustomerSegmentTotalsResponse, error){
	"default": (*HouseholdApprovalServer).countCustomerSegmentTotalsStep,
	"audited": (*HouseholdApprovalServer).countCustomerSegmentTotalsAuditedStep,
}

func (s *HouseholdApprovalServer) countCustomerSegmentTotalsStep(ctx context.Context, request *customersv1.CountCustomerSegmentTotalsRequest) (*customersv1.CountCustomerSegmentTotalsResponse, error) {
	return s.customerSegmentReportingClient.CountCustomerSegmentTotals(ctx, request)
}

func (s *HouseholdApprovalServer) countCustomerSegmentTotalsAuditedStep(ctx context.Context, request *customersv1.CountCustomerSegmentTotalsRequest) (*customersv1.CountCustomerSegmentTotalsResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.customers.v1.CustomerSegmentReportingService/CountCustomerSegmentTotals")
	return s.customerSegmentReportingClient.CountCustomerSegmentTotals(ctx, request)
}

// householdApprovalServerCommentHouseholdApprovalBatchGetNotificationsStep carries the request of one step of
// CommentHouseholdApproval and, once it ran, its reply.
type householdApprovalServerCommentHouseholdApprovalBatchGetNotificationsStep struct {
	request *notificationsv1.BatchGetNotificationsRequest
	reply   *notificationsv1.BatchGetNotificationsResponse
}

func (s *HouseholdApprovalServer) runCommentHouseholdApprovalBatchGetNotifications(ctx context.Context, step *householdApprovalServerCommentHouseholdApprovalBatchGetNotificationsStep) error {
	reply, err := s.notificationClient.BatchGetNotifications(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
