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
	mediav1 "github.com/hounddogai/protomap-test-golang/gen/acme/media/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	onboardingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/onboarding/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// InspectionLifecycleServer implements `acme.returns.v1.InspectionLifecycleService`.
// Moves an inspection between the states of its lifecycle.
type InspectionLifecycleServer struct {
	*inspectionLifecycleHandlers
}

// inspectionLifecycleHandlers holds the handlers of InspectionLifecycleServer, with what they use.
type inspectionLifecycleHandlers struct {
	returnsv1.UnimplementedInspectionLifecycleServiceServer
	store                                   *Store
	log                                     *slog.Logger
	sellerListingApprovalClient             sellersv1.SellerListingApprovalServiceClient
	customerCalculationClient               customersv1.CustomerCalculationServiceClient
	returnRequestQueryClient                returnsv1.ReturnRequestQueryServiceClient
	notificationValidationClient            notificationsv1.NotificationValidationServiceClient
	countOnboardingPlansFn                  func(context.Context, *onboardingv1.CountOnboardingPlansRequest, ...grpc.CallOption) (*onboardingv1.CountOnboardingPlansResponse, error)
	merchandisingRuleWorkflowClient         merchandisingv1.MerchandisingRuleWorkflowServiceClient
	searchIndexValidationClient             searchv1.SearchIndexValidationServiceClient
	returnRequestValidationClient           returnsv1.ReturnRequestValidationServiceClient
	consentRecordCalculationClient          customersv1.ConsentRecordCalculationServiceClient
	customerSegmentValidationClient         customersv1.CustomerSegmentValidationServiceClient
	metricSeriesClient                      metricsv1.MetricSeriesServiceClient
	returnRequestClient                     returnsv1.ReturnRequestServiceClient
	conn                                    *grpc.ClientConn
	inspectionCalculationClient             returnsv1.InspectionCalculationServiceClient
	merchandisingRuleWorkflowClientFallback merchandisingv1.MerchandisingRuleWorkflowServiceClient
	mediaAssetClient                        mediav1.MediaAssetServiceClient
	restockDecisionValidationClient         returnsv1.RestockDecisionValidationServiceClient
}

// NewInspectionLifecycleServer wires an InspectionLifecycleServer.
func NewInspectionLifecycleServer(deps platform.Deps) *InspectionLifecycleServer {
	onboardingPlanQuery := onboardingv1.NewOnboardingPlanQueryServiceClient(deps.Conn)
	return &InspectionLifecycleServer{inspectionLifecycleHandlers: &inspectionLifecycleHandlers{
		store:                                   NewStore(deps.DB),
		log:                                     deps.Logger.With("service", "acme.returns.v1.InspectionLifecycleService"),
		sellerListingApprovalClient:             sellersv1.NewSellerListingApprovalServiceClient(deps.Conn),
		customerCalculationClient:               customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		returnRequestQueryClient:                returnsv1.NewReturnRequestQueryServiceClient(deps.Conn),
		notificationValidationClient:            notificationsv1.NewNotificationValidationServiceClient(deps.Conn),
		countOnboardingPlansFn:                  onboardingPlanQuery.CountOnboardingPlans,
		merchandisingRuleWorkflowClient:         merchandisingv1.NewMerchandisingRuleWorkflowServiceClient(deps.Conn),
		searchIndexValidationClient:             searchv1.NewSearchIndexValidationServiceClient(deps.Conn),
		returnRequestValidationClient:           returnsv1.NewReturnRequestValidationServiceClient(deps.Conn),
		consentRecordCalculationClient:          customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
		customerSegmentValidationClient:         customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn),
		metricSeriesClient:                      metricsv1.NewMetricSeriesServiceClient(deps.Conn),
		returnRequestClient:                     returnsv1.NewReturnRequestServiceClient(deps.Conn),
		conn:                                    deps.Conn,
		inspectionCalculationClient:             returnsv1.NewInspectionCalculationServiceClient(deps.Conn),
		merchandisingRuleWorkflowClientFallback: merchandisingv1.NewMerchandisingRuleWorkflowServiceClient(deps.Conn),
		mediaAssetClient:                        mediav1.NewMediaAssetServiceClient(deps.Conn),
		restockDecisionValidationClient:         returnsv1.NewRestockDecisionValidationServiceClient(deps.Conn),
	}}
}

// ActivateInspection activates an inspection.
func (s *inspectionLifecycleHandlers) ActivateInspection(ctx context.Context, req *returnsv1.ActivateInspectionRequest) (*returnsv1.ActivateInspectionResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "inspection", req.GetInspectionId(), "activate_inspection"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate inspection", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "user_id", req.GetDetails().GetNewOwner().GetUserId())
	recallSellerListingRequest := &sellersv1.RecallSellerListingRequest{
		Context: req.GetContext(),
		Details: &sellersv1.RecallSellerListingRequest_Details{
			Approver:    &identityv1.UserRef{Email: req.GetDetails().GetNotifyContact().GetEmail()},
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{
				Email:  req.GetDetails().GetNewOwner().GetEmail(),
				UserId: tenancy.Value(ctx, "user_id"),
			},
		},
	}
	recallSellerListingReply, err := grpcx.Call(ctx, s.sellerListingApprovalClient.RecallSellerListing, recallSellerListingRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingApprovalService/RecallSellerListing")
	}
	recalculateCustomersReply, err := s.customerCalculationClient.RecalculateCustomers(ctx, &customersv1.RecalculateCustomersRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			PrimaryAddress: &commonv1.PostalAddress{
				Recipient: recallSellerListingReply.GetSellerListing().GetAddress().GetRecipient(),
			},
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNewOwner().GetEmail()},
			RelationshipManager: &identityv1.UserRef{
				DisplayName: recallSellerListingReply.GetSellerListing().GetOwner().GetDisplayName(),
			},
		},
		Details: &customersv1.RecalculateCustomersRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region: recallSellerListingReply.GetSellerListing().GetAddress().GetRegion(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/RecalculateCustomers")
	}
	return &returnsv1.ActivateInspectionResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: recallSellerListingReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &returnsv1.ActivateInspectionResponse_Outcome{
			Message:     recalculateCustomersReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Inspection: &returnsv1.Inspection{
			Assignee: &identityv1.UserRef{
				DisplayName: recalculateCustomersReply.GetCustomer().GetRelationshipManager().GetDisplayName(),
			},
			InspectionId: req.GetInspectionId(),
		},
	}, nil
}

// SuspendInspection suspends an inspection.
func (s *inspectionLifecycleHandlers) SuspendInspection(ctx context.Context, req *returnsv1.SuspendInspectionRequest) (*returnsv1.SuspendInspectionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuspendInspection", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "inspection", req.GetInspectionId(), "suspend_inspection"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend inspection", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	exportReturnRequestsStream, err := s.returnRequestQueryClient.ExportReturnRequests(ctx, &returnsv1.ExportReturnRequestsRequest{
		Context: req.GetContext(),
		Filter: &returnsv1.ExportReturnRequestsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			Labels:       annotations,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestQueryService/ExportReturnRequests")
	}
	for {
		received, err := exportReturnRequestsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestQueryService/ExportReturnRequests")
		}
		_ = received
	}
	detailsCache := cache.New[string, *returnsv1.SuspendInspectionRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	explainNotificationDecisionRequest := &notificationsv1.ExplainNotificationDecisionRequest{
		Context: req.GetContext(),
		Notification: &notificationsv1.Notification{
			Assignee:  &identityv1.UserRef{Email: req.GetDetails().GetNotifyContact().GetEmail()},
			Requester: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotifyContact().GetPhone()},
		},
		Details: &notificationsv1.ExplainNotificationDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: cachedDetails.GetNotifyContact().GetPreferredChannel(),
			},
		},
	}
	explainNotificationDecisionReply, err := grpcx.Call(ctx, s.notificationValidationClient.ExplainNotificationDecision, explainNotificationDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationValidationService/ExplainNotificationDecision")
	}
	return &returnsv1.SuspendInspectionResponse{
		Outcome: &returnsv1.SuspendInspectionResponse_Outcome{
			Message:     explainNotificationDecisionReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Inspection: &returnsv1.Inspection{InspectionId: req.GetInspectionId()},
		Result:     &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ReinstateInspection reinstates an inspection.
func (s *inspectionLifecycleHandlers) ReinstateInspection(ctx context.Context, req *returnsv1.ReinstateInspectionRequest) (*returnsv1.ReinstateInspectionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ReinstateInspection", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "inspection", req.GetInspectionId(), "reinstate_inspection"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate inspection", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	countOnboardingPlansRequest := &onboardingv1.CountOnboardingPlansRequest{
		Context: req.GetContext(),
		Filter: &onboardingv1.CountOnboardingPlansRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetInspectionId(),
			Labels:       annotations,
		},
	}
	countOnboardingPlansReply, err := s.countOnboardingPlansFn(ctx, countOnboardingPlansRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.onboarding.v1.OnboardingPlanQueryService/CountOnboardingPlans")
	}
	var holder struct{ Email string }
	if record := req.GetDetails().GetNewOwner(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	ctx = tenancy.With(ctx, "phone", req.GetDetails().GetNotifyContact().GetPhone())
	detailsCache := cache.New[string, *returnsv1.ReinstateInspectionRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	startMerchandisingRuleWorkflowReply, err := s.merchandisingRuleWorkflowClient.StartMerchandisingRuleWorkflow(ctx, &merchandisingv1.StartMerchandisingRuleWorkflowRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		Details: &merchandisingv1.StartMerchandisingRuleWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email:            holder.Email,
				Phone:            tenancy.Value(ctx, "phone"),
				PreferredChannel: cachedDetails.GetNotifyContact().GetPreferredChannel(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleWorkflowService/StartMerchandisingRuleWorkflow")
	}
	var holder2 struct{ PreferredChannel string }
	if record2 := req.GetDetails().GetNotifyContact(); record2 != nil {
		if err := mapper.CopyFields(&holder2, record2, "PreferredChannel"); err != nil {
			s.log.WarnContext(ctx, "copy preferred_channel", "error", err)
		}
	}
	explainSearchIndexDecisionRequest := &searchv1.ExplainSearchIndexDecisionRequest{
		Context: req.GetContext(),
		Details: &searchv1.ExplainSearchIndexDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: holder2.PreferredChannel,
				Name:             req.GetDetails().GetNotifyContact().GetName(),
			},
		},
		SearchIndex: &searchv1.SearchIndex{
			Audit: &commonv1.AuditInfo{
				CreatedBy: startMerchandisingRuleWorkflowReply.GetMerchandisingRule().GetAudit().GetCreatedBy(),
				UpdatedBy: startMerchandisingRuleWorkflowReply.GetMerchandisingRule().GetAudit().GetUpdatedBy(),
			},
		},
	}
	type explainSearchIndexDecisionResult struct {
		reply *searchv1.ExplainSearchIndexDecisionResponse
		err   error
	}
	explainSearchIndexDecisionResults := make(chan explainSearchIndexDecisionResult, 1)
	go func() {
		reply, err := s.searchIndexValidationClient.ExplainSearchIndexDecision(ctx, explainSearchIndexDecisionRequest)
		explainSearchIndexDecisionResults <- explainSearchIndexDecisionResult{reply, err}
	}()
	var explainSearchIndexDecisionReply *searchv1.ExplainSearchIndexDecisionResponse
	select {
	case result := <-explainSearchIndexDecisionResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.search.v1.SearchIndexValidationService/ExplainSearchIndexDecision")
		}
		explainSearchIndexDecisionReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	normalizeReturnRequestRequest := &returnsv1.NormalizeReturnRequestRequest{
		Context: req.GetContext(),
		Details: &returnsv1.NormalizeReturnRequestRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotifyContact().GetName()},
		},
		ReturnRequest: &returnsv1.ReturnRequest{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: startMerchandisingRuleWorkflowReply.GetMerchandisingRule().GetOwner().GetDisplayName(),
			},
			Lines: []*returnsv1.ReturnRequestLine{&returnsv1.ReturnRequestLine{
				Description: explainSearchIndexDecisionReply.GetSearchIndex().GetComponents()["primary"].GetDescription(),
			}},
		},
	}
	normalizeReturnRequestReply, err := returnsv1.ReturnRequestValidationServiceClient.NormalizeReturnRequest(s.returnRequestValidationClient, ctx, normalizeReturnRequestRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestValidationService/NormalizeReturnRequest")
	}
	return &returnsv1.ReinstateInspectionResponse{
		Outcome: &returnsv1.ReinstateInspectionResponse_Outcome{
			Status:      countOnboardingPlansReply.GetResult().GetStatus(),
			ReferenceId: normalizeReturnRequestReply.GetOutcome().GetReferenceId(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: startMerchandisingRuleWorkflowReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Inspection: &returnsv1.Inspection{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: explainSearchIndexDecisionReply.GetSearchIndex().GetAudit().GetUpdatedAt(),
				UpdatedBy: normalizeReturnRequestReply.GetReturnRequest().GetAudit().GetUpdatedBy(),
			},
			InspectionId: req.GetInspectionId(),
		},
	}, nil
}

// TerminateInspection terminates an inspection.
func (s *inspectionLifecycleHandlers) TerminateInspection(ctx context.Context, req *returnsv1.TerminateInspectionRequest) (*returnsv1.TerminateInspectionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "inspection", req.GetInspectionId(), "terminate_inspection"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate inspection", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "user_id", req.GetDetails().GetNewOwner().GetUserId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	listPendingTiersRequest := &loyaltyv1.ListPendingTiersRequest{
		Context: req.GetContext(),
		Filter: &loyaltyv1.ListPendingTiersRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      tenancy.Value(ctx, "user_id"),
			Labels:       annotations,
		},
	}
	listPendingTiersReply, err := listPendingTiersCall(ctx, listPendingTiersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierApprovalService/ListPendingTiers")
	}
	annotations2 := make(map[string]string, len(req.GetAnnotations()))
	for key2, value2 := range req.GetAnnotations() {
		annotations2[key2] = value2
	}
	recalculateConsentRecordsReply, err := s.consentRecordCalculationClient.RecalculateConsentRecords(ctx, &customersv1.RecalculateConsentRecordsRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			CorrelationId: listPendingTiersReply.GetResult().GetCorrelationId(),
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		Reason:      req.GetReason(),
		Annotations: annotations2,
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/RecalculateConsentRecords")
	}
	return &returnsv1.TerminateInspectionResponse{
		Outcome: &returnsv1.TerminateInspectionResponse_Outcome{
			Status:      listPendingTiersReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Status: recalculateConsentRecordsReply.GetResult().GetStatus(),
		},
		Inspection: &returnsv1.Inspection{
			Assignee: &identityv1.UserRef{
				DisplayName: recalculateConsentRecordsReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
			InspectionId: req.GetInspectionId(),
		},
	}, nil
}

// RenewInspection renews an inspection.
func (s *inspectionLifecycleHandlers) RenewInspection(ctx context.Context, req *returnsv1.RenewInspectionRequest) (*returnsv1.RenewInspectionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "inspection", req.GetInspectionId(), "renew_inspection"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew inspection", "tenant", req.GetContext().GetTenantId())
	lintCustomerSegmentsReply, err := s.customerSegmentValidationClient.LintCustomerSegments(ctx, &customersv1.LintCustomerSegmentsRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		Details: &customersv1.LintCustomerSegmentsRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
				Email:            req.GetDetails().GetNewOwner().GetEmail(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/LintCustomerSegments")
	}
	return &returnsv1.RenewInspectionResponse{
		Inspection: &returnsv1.Inspection{
			Audit: &commonv1.AuditInfo{
				CreatedBy: lintCustomerSegmentsReply.GetCustomerSegment().GetAudit().GetCreatedBy(),
			},
			InspectionId: req.GetInspectionId(),
		},
		Outcome: &returnsv1.RenewInspectionResponse_Outcome{
			ReferenceId: lintCustomerSegmentsReply.GetOutcome().GetReferenceId(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExpireInspection expires an inspection.
func (s *inspectionLifecycleHandlers) ExpireInspection(ctx context.Context, req *returnsv1.ExpireInspectionRequest) (*returnsv1.ExpireInspectionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExpireInspection", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "inspection", req.GetInspectionId(), "expire_inspection"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire inspection", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*returnsv1.ExpireInspectionRequest_Details)
	watchMetricSeriesesStream, err := s.metricSeriesClient.WatchMetricSerieses(ctx, &metricsv1.WatchMetricSeriesesRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Name:             cloned.GetNotifyContact().GetName(),
			PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesService/WatchMetricSerieses")
	}
	for {
		received, err := watchMetricSeriesesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesService/WatchMetricSerieses")
		}
		_ = received
	}
	source := req.GetDetails().GetNewOwner().ProtoReflect()
	userId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("user_id"))).String()
	searchReturnRequestsRequest := &returnsv1.SearchReturnRequestsRequest{
		Context: req.GetContext(),
		Filter: &returnsv1.SearchReturnRequestsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      userId,
			Labels:       req.GetContext().GetBaggage(),
		},
	}
	searchReturnRequestsReply, err := grpcx.Call(ctx, s.returnRequestClient.SearchReturnRequests, searchReturnRequestsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestService/SearchReturnRequests")
	}
	var holder struct{ Email string }
	if record := req.GetDetails().GetNotifyContact(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	recalculateCustomersRequest := &customersv1.RecalculateCustomersRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Contact: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetNotifyContact().GetPhone(),
				Email: holder.Email,
			},
			RelationshipManager: &identityv1.UserRef{UserId: req.GetDetails().GetNewOwner().GetUserId()},
		},
	}
	recalculateCustomersReply := new(customersv1.RecalculateCustomersResponse)
	if err := s.conn.Invoke(ctx, customersCustomerCalculationServiceRecalculateCustomersMethod, recalculateCustomersRequest, recalculateCustomersReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/RecalculateCustomers")
	}
	return &returnsv1.ExpireInspectionResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: searchReturnRequestsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Inspection: &returnsv1.Inspection{
			Audit: &commonv1.AuditInfo{
				CreatedBy: recalculateCustomersReply.GetCustomer().GetAudit().GetCreatedBy(),
			},
			InspectionId: req.GetInspectionId(),
		},
		Outcome: &returnsv1.ExpireInspectionResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// GetInspectionLifecycle returns an inspection.
func (s *inspectionLifecycleHandlers) GetInspectionLifecycle(ctx context.Context, req *returnsv1.GetInspectionLifecycleRequest) (*returnsv1.GetInspectionLifecycleResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "inspection", req.GetInspectionId(), "get_inspection_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get inspection lifecycle", "tenant", req.GetContext().GetTenantId())
	calculateInspectionReply, err := s.inspectionCalculationClient.CalculateInspection(ctx, &returnsv1.CalculateInspectionRequest{
		Context: req.GetContext(),
		Inspection: &returnsv1.Inspection{
			InspectionId: req.GetInspectionId(),
			Assignee:     &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Requester:    &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			History:      []*returnsv1.InspectionStatusChange{&returnsv1.InspectionStatusChange{ChangedAt: req.GetContext().GetDeadline()}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionCalculationService/CalculateInspection")
	}
	return &returnsv1.GetInspectionLifecycleResponse{
		Inspection: &returnsv1.Inspection{
			InspectionId: calculateInspectionReply.GetInspection().GetInspectionId(),
			Audit: &commonv1.AuditInfo{
				Etag: calculateInspectionReply.GetInspection().GetAudit().GetEtag(),
			},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScheduleInspectionExpiry schedules an inspection.
func (s *inspectionLifecycleHandlers) ScheduleInspectionExpiry(ctx context.Context, req *returnsv1.ScheduleInspectionExpiryRequest) (*returnsv1.ScheduleInspectionExpiryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveInspection(ctx, req.GetContext().GetTenantId(), req.GetInspection()); err != nil {
		return nil, grpcx.Wrap(err, "save inspection")
	}
	s.log.InfoContext(ctx, "schedule inspection expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetInspection().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *merchandisingv1.AdvanceMerchandisingRuleWorkflowRequest, ...grpc.CallOption) (*merchandisingv1.AdvanceMerchandisingRuleWorkflowResponse, error){
		"primary":  s.merchandisingRuleWorkflowClient.AdvanceMerchandisingRuleWorkflow,
		"fallback": s.merchandisingRuleWorkflowClientFallback.AdvanceMerchandisingRuleWorkflow,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	ctx = tenancy.With(ctx, "user_id", req.GetInspection().GetAssignee().GetUserId())
	advanceMerchandisingRuleWorkflowRequest := &merchandisingv1.AdvanceMerchandisingRuleWorkflowRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetInspection().GetAudit().GetUpdatedAt()},
			Owner: &identityv1.UserRef{UserId: tenancy.Value(ctx, "user_id")},
		},
	}
	advanceMerchandisingRuleWorkflowReply, err := route(ctx, advanceMerchandisingRuleWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleWorkflowService/AdvanceMerchandisingRuleWorkflow")
	}
	for comment := range iterx.Each(req.GetInspection().GetComments(), func(candidate *returnsv1.InspectionComment) bool { return candidate != nil }) {
		if _, err := s.mediaAssetClient.SearchMediaAssets(ctx, &mediav1.SearchMediaAssetsRequest{
			Context: req.GetContext(),
			Filter: &mediav1.SearchMediaAssetsRequest_Filter{
				CreatedAfter: comment.GetCreatedAt(),
				OwnerId:      comment.GetCommentId(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.media.v1.MediaAssetService/SearchMediaAssets")
		}
	}
	validateRestockDecisionRequest := &returnsv1.ValidateRestockDecisionRequest{}
	validateRestockDecisionRequest.Context = req.GetContext()
	validateRestockDecisionRequest.RestockDecision = &returnsv1.RestockDecision{
		Action: &returnsv1.RestockDecision_Block{Block: &returnsv1.RestockDecisionBlockAction{
			NotifyEmail: advanceMerchandisingRuleWorkflowReply.GetMerchandisingRule().GetBlock().GetNotifyEmail(),
		}},
		Audit: &commonv1.AuditInfo{Etag: req.GetInspection().GetAudit().GetEtag()},
		Owner: &identityv1.UserRef{
			DisplayName: req.GetInspection().GetAssignee().GetDisplayName(),
		},
	}
	validateRestockDecisionReply, err := s.restockDecisionValidationClient.ValidateRestockDecision(ctx, validateRestockDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionValidationService/ValidateRestockDecision")
	}
	return &returnsv1.ScheduleInspectionExpiryResponse{
		Result: &commonv1.OperationResult{
			Message: advanceMerchandisingRuleWorkflowReply.GetOutcome().GetMessage(),
			Details: advanceMerchandisingRuleWorkflowReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Inspection: &returnsv1.Inspection{
			Assignee: &identityv1.UserRef{
				Email: validateRestockDecisionReply.GetRestockDecision().GetOwner().GetEmail(),
			},
			InspectionId: req.GetInspectionId(),
			CustomFields: req.GetInspection().GetCustomFields(),
		},
	}, nil
}
