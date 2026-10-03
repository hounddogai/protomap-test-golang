package warehousing

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	onboardingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/onboarding/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PickListApprovalServer implements `acme.warehousing.v1.PickListApprovalService`.
// Routes pick lists through approval, with escalation and delegation.
type PickListApprovalServer struct {
	warehousingv1.UnimplementedPickListApprovalServiceServer
	store                         *Store
	log                           *slog.Logger
	storageBinApprovalClient      warehousingv1.StorageBinApprovalServiceClient
	notificationValidationClient  notificationsv1.NotificationValidationServiceClient
	synonymSyncClient             searchv1.SynonymSyncServiceClient
	warehouseClient               warehousingv1.WarehouseServiceClient
	searchQueryHistoryClient      searchv1.SearchQueryHistoryServiceClient
	householdQueryClient          customersv1.HouseholdQueryServiceClient
	putawayTaskValidationClient   warehousingv1.PutawayTaskValidationServiceClient
	customerQueryClient           customersv1.CustomerQueryServiceClient
	synonymSyncClientFallback     searchv1.SynonymSyncServiceClient
	preferenceValidationPort      preferenceValidationPort
	consentRecordValidationClient customersv1.ConsentRecordValidationServiceClient
	conn                          *grpc.ClientConn
	clients                       *registry.Registry
	fulfillmentWorkflowPort       fulfillmentWorkflowPort
	rewardApprovalClient          loyaltyv1.RewardApprovalServiceClient
}

// NewPickListApprovalServer wires a PickListApprovalServer.
func NewPickListApprovalServer(deps platform.Deps) *PickListApprovalServer {
	return &PickListApprovalServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.warehousing.v1.PickListApprovalService"),
		storageBinApprovalClient:      warehousingv1.NewStorageBinApprovalServiceClient(deps.Conn),
		notificationValidationClient:  notificationsv1.NewNotificationValidationServiceClient(deps.Conn),
		synonymSyncClient:             searchv1.NewSynonymSyncServiceClient(deps.Conn),
		warehouseClient:               warehousingv1.NewWarehouseServiceClient(deps.Conn),
		searchQueryHistoryClient:      searchv1.NewSearchQueryHistoryServiceClient(deps.Conn),
		householdQueryClient:          customersv1.NewHouseholdQueryServiceClient(deps.Conn),
		putawayTaskValidationClient:   warehousingv1.NewPutawayTaskValidationServiceClient(deps.Conn),
		customerQueryClient:           customersv1.NewCustomerQueryServiceClient(deps.Conn),
		synonymSyncClientFallback:     searchv1.NewSynonymSyncServiceClient(deps.Conn),
		preferenceValidationPort:      newPreferenceValidationPort(deps),
		consentRecordValidationClient: customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		conn:                          deps.Conn,
		clients:                       deps.Clients,
		fulfillmentWorkflowPort:       newFulfillmentWorkflowPort(deps),
		rewardApprovalClient:          loyaltyv1.NewRewardApprovalServiceClient(deps.Conn),
	}
}

// SubmitPickList submits a pick list.
func (s *PickListApprovalServer) SubmitPickList(ctx context.Context, req *warehousingv1.SubmitPickListRequest) (*warehousingv1.SubmitPickListResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubmitPickList", "error", err)
		return nil, err
	}
	if err := s.store.SavePickList(ctx, req.GetContext().GetTenantId(), req.GetPickList()); err != nil {
		return nil, grpcx.Wrap(err, "save pick list")
	}
	s.log.InfoContext(ctx, "submit pick list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPickList().GetAudit().GetEtag())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := pickListApprovalServerRecallStorageBinSteps[mode]
	recallStorageBinRequest := &warehousingv1.RecallStorageBinRequest{
		Context: req.GetContext(),
		StorageBin: &warehousingv1.StorageBin{
			Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
				Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
					Location: &commonv1.PostalAddress{
						Recipient:   req.GetPickList().GetBillingAddress().GetRecipient(),
						CountryCode: req.GetPickList().GetBillingAddress().GetCountryCode(),
					},
				}},
			}},
		},
	}
	recallStorageBinReply, err := run(s, ctx, recallStorageBinRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinApprovalService/RecallStorageBin")
	}
	return &warehousingv1.SubmitPickListResponse{
		PickList: &warehousingv1.PickList{
			Audit: &commonv1.AuditInfo{
				Etag: recallStorageBinReply.GetStorageBin().GetAudit().GetEtag(),
			},
			PickListId: req.GetPickListId(),
			Metadata:   req.GetPickList().GetMetadata(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ApprovePickList approves a pick list.
func (s *PickListApprovalServer) ApprovePickList(ctx context.Context, req *warehousingv1.ApprovePickListRequest) (*warehousingv1.ApprovePickListResponse, error) {
	if err := s.store.SavePickList(ctx, req.GetContext().GetTenantId(), req.GetPickList()); err != nil {
		return nil, grpcx.Wrap(err, "save pick list")
	}
	s.log.InfoContext(ctx, "approve pick list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPickList().GetAudit().GetEtag())
	previewNotificationRequest := buildPreviewNotificationRequestForApprovePickList(req)
	previewNotificationReply, err := s.notificationValidationClient.PreviewNotification(ctx, previewNotificationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationValidationService/PreviewNotification")
	}
	return &warehousingv1.ApprovePickListResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: previewNotificationReply.GetResult().GetCorrelationId(),
			Message:       previewNotificationReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		PickList: &warehousingv1.PickList{
			PickListId: req.GetPickListId(),
			Metadata:   req.GetPickList().GetMetadata(),
		},
	}, nil
}

// RejectPickList rejects a pick list.
func (s *PickListApprovalServer) RejectPickList(ctx context.Context, req *warehousingv1.RejectPickListRequest) (*warehousingv1.RejectPickListResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePickList(ctx, req.GetContext().GetTenantId(), req.GetPickList()); err != nil {
		return nil, grpcx.Wrap(err, "save pick list")
	}
	s.log.InfoContext(ctx, "reject pick list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPickList().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetPickList()).(*warehousingv1.PickList)
	listSynonymConflictsReply, err := s.synonymSyncClient.ListSynonymConflicts(ctx, &searchv1.ListSynonymConflictsRequest{
		Context: req.GetContext(),
		Filter:  &searchv1.ListSynonymConflictsRequest_Filter{Region: cloned.GetBillingAddress().GetRegion()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/ListSynonymConflicts")
	}
	searchWarehousesRequest := &warehousingv1.SearchWarehousesRequest{
		Context: req.GetContext(),
		Filter: &warehousingv1.SearchWarehousesRequest_Filter{
			Region:       req.GetPickList().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetPickList().GetDueAt(),
			OwnerId:      req.GetPickList().GetPickListId(),
			Labels:       listSynonymConflictsReply.GetResult().GetDetails(),
		},
	}
	searchWarehousesReply, err := grpcx.Call(ctx, s.warehouseClient.SearchWarehouses, searchWarehousesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.WarehouseService/SearchWarehouses")
	}
	return &warehousingv1.RejectPickListResponse{
		Result: &commonv1.OperationResult{
			Message: listSynonymConflictsReply.GetResult().GetMessage(),
			Details: listSynonymConflictsReply.GetResult().GetDetails(),
			Status:  searchWarehousesReply.GetResult().GetStatus(),
		},
		PickList: &warehousingv1.PickList{
			PickListId: req.GetPickListId(),
			DueAt:      req.GetPickList().GetDueAt(),
		},
	}, nil
}

// EscalatePickList escalates a pick list.
func (s *PickListApprovalServer) EscalatePickList(ctx context.Context, req *warehousingv1.EscalatePickListRequest) (*warehousingv1.EscalatePickListResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePickList(ctx, req.GetContext().GetTenantId(), req.GetPickList()); err != nil {
		return nil, grpcx.Wrap(err, "save pick list")
	}
	s.log.InfoContext(ctx, "escalate pick list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPickList().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetPickList())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	snapshot, err := protojson.Marshal(req.GetPickList())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	revertSearchQueryRequest := &searchv1.RevertSearchQueryRequest{
		Context: req.GetContext(),
		SearchQuery: &searchv1.SearchQuery{
			Subject: &searchv1.SearchIndexRef{
				DisplayName: req.GetPickList().GetCounterparty().GetDisplayName(),
			},
			Detail: packed,
		},
		Annotations: req.GetAnnotations(),
		Details:     &searchv1.RevertSearchQueryRequest_Details{Snapshot: snapshot},
	}
	revertSearchQueryReply, err := searchv1.SearchQueryHistoryServiceClient.RevertSearchQuery(s.searchQueryHistoryClient, ctx, revertSearchQueryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryHistoryService/RevertSearchQuery")
	}
	pickListCache := cache.New[string, *warehousingv1.PickList](1)
	pickListCache.Put(req.GetContext().GetTraceId(), req.GetPickList())
	cachedPickList, _ := pickListCache.Get(req.GetContext().GetTraceId())
	findHouseholdsReply, err := s.householdQueryClient.FindHouseholds(ctx, &customersv1.FindHouseholdsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.FindHouseholdsRequest_Filter{
			Region:       req.GetPickList().GetBillingAddress().GetRegion(),
			CreatedAfter: cachedPickList.GetDueAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/FindHouseholds")
	}
	validatePutawayTaskRequest := &warehousingv1.ValidatePutawayTaskRequest{
		Context: req.GetContext(),
		PutawayTask: &warehousingv1.PutawayTask{
			Origin: &warehousingv1.PutawayTask_Customer{Customer: &warehousingv1.StorageBinRef{
				DisplayName: req.GetPickList().GetCounterparty().GetDisplayName(),
			}},
			Comments:  []*warehousingv1.PutawayTaskComment{&warehousingv1.PutawayTaskComment{CreatedAt: req.GetPickList().GetAudit().GetCreatedAt()}},
			Requester: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	send := s.validatePutawayTaskSender(validatePutawayTaskRequest)
	validatePutawayTaskReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PutawayTaskValidationService/ValidatePutawayTask")
	}
	exportCustomersStream, err := s.customerQueryClient.ExportCustomers(ctx, &customersv1.ExportCustomersRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ExportCustomersRequest_Filter{
			Region:       req.GetPickList().GetBillingAddress().GetRegion(),
			CreatedAfter: validatePutawayTaskReply.GetPutawayTask().GetAudit().GetCreatedAt(),
			OwnerId:      validatePutawayTaskReply.GetPutawayTask().GetAssignee().GetUserId(),
			Labels:       revertSearchQueryReply.GetResult().GetDetails(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/ExportCustomers")
	}
	for {
		received, err := exportCustomersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/ExportCustomers")
		}
		_ = received
	}
	return &warehousingv1.EscalatePickListResponse{
		Result: &commonv1.OperationResult{
			Status:        revertSearchQueryReply.GetResult().GetStatus(),
			CorrelationId: findHouseholdsReply.GetResult().GetCorrelationId(),
			Details:       findHouseholdsReply.GetResult().GetDetails(),
		},
		PickList: &warehousingv1.PickList{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: validatePutawayTaskReply.GetPutawayTask().GetAssignee().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: validatePutawayTaskReply.GetPutawayTask().GetAudit().GetUpdatedAt(),
			},
			PickListId: req.GetPickListId(),
			Status:     req.GetPickList().GetStatus(),
		},
	}, nil
}

// ListPendingPickLists lists pick lists a page at a time.
func (s *PickListApprovalServer) ListPendingPickLists(ctx context.Context, req *warehousingv1.ListPendingPickListsRequest) (*warehousingv1.ListPendingPickListsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListPendingPickLists", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending pick lists", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *searchv1.ListSynonymConflictsRequest, ...grpc.CallOption) (*searchv1.ListSynonymConflictsResponse, error){
		"primary":  s.synonymSyncClient.ListSynonymConflicts,
		"fallback": s.synonymSyncClientFallback.ListSynonymConflicts,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	listSynonymConflictsRequest := &searchv1.ListSynonymConflictsRequest{
		Context: req.GetContext(),
		Page: &commonv1.PageRequest{
			PageSize:  req.GetPage().GetPageSize(),
			PageToken: req.GetPage().GetPageToken(),
		},
		Filter: &searchv1.ListSynonymConflictsRequest_Filter{
			Region: req.GetFilter().GetRegion(),
			Labels: labels,
		},
	}
	listSynonymConflictsReply, err := route(ctx, listSynonymConflictsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/ListSynonymConflicts")
	}
	return &warehousingv1.ListPendingPickListsResponse{
		PageInfo: &commonv1.PageInfo{
			NextPageToken: listSynonymConflictsReply.GetPageInfo().GetNextPageToken(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// DelegatePickListApproval delegates a pick list.
func (s *PickListApprovalServer) DelegatePickListApproval(ctx context.Context, req *warehousingv1.DelegatePickListApprovalRequest) (*warehousingv1.DelegatePickListApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePickList(ctx, req.GetContext().GetTenantId(), req.GetPickList()); err != nil {
		return nil, grpcx.Wrap(err, "save pick list")
	}
	s.log.InfoContext(ctx, "delegate pick list approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPickList().GetAudit().GetEtag())
	checkPutawayTaskEligibilityRequest := &warehousingv1.CheckPutawayTaskEligibilityRequest{
		Context: req.GetContext(),
		PutawayTask: &warehousingv1.PutawayTask{
			Origin: &warehousingv1.PutawayTask_Customer{Customer: &warehousingv1.StorageBinRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			}},
		},
	}
	send := s.checkPutawayTaskEligibilitySender(checkPutawayTaskEligibilityRequest)
	checkPutawayTaskEligibilityReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PutawayTaskValidationService/CheckPutawayTaskEligibility")
	}
	explainPreferenceDecisionRequest := &customersv1.ExplainPreferenceDecisionRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetPickList().GetAudit().GetUpdatedBy()},
			Owner: &identityv1.UserRef{
				DisplayName: checkPutawayTaskEligibilityReply.GetPutawayTask().GetCustomer().GetDisplayName(),
				UserId:      req.GetDetails().GetApprover().GetUserId(),
			},
		},
		Details: &customersv1.ExplainPreferenceDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: checkPutawayTaskEligibilityReply.GetPutawayTask().GetRequester().GetEmail(),
			},
		},
	}
	explainPreferenceDecisionReply, err := s.preferenceValidationPort.ExplainPreferenceDecision(ctx, explainPreferenceDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/ExplainPreferenceDecision")
	}
	return &warehousingv1.DelegatePickListApprovalResponse{
		Outcome: &warehousingv1.DelegatePickListApprovalResponse_Outcome{
			ReferenceId: checkPutawayTaskEligibilityReply.GetOutcome().GetReferenceId(),
			Status:      explainPreferenceDecisionReply.GetResult().GetStatus(),
			Message:     explainPreferenceDecisionReply.GetResult().GetMessage(),
		},
		PickList: &warehousingv1.PickList{
			PickListId:     req.GetPickListId(),
			BillingAddress: req.GetPickList().GetBillingAddress(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecallPickList recalls a pick list.
func (s *PickListApprovalServer) RecallPickList(ctx context.Context, req *warehousingv1.RecallPickListRequest) (*warehousingv1.RecallPickListResponse, error) {
	if err := s.store.SavePickList(ctx, req.GetContext().GetTenantId(), req.GetPickList()); err != nil {
		return nil, grpcx.Wrap(err, "save pick list")
	}
	s.log.InfoContext(ctx, "recall pick list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPickList().GetAudit().GetEtag())
	jobs := make(chan *warehousingv1.PickListLine)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range jobs {
				if _, err := s.consentRecordValidationClient.PreviewConsentRecord(workCtx, &customersv1.PreviewConsentRecordRequest{
					Context: req.GetContext(),
					ConsentRecord: &customersv1.ConsentRecord{
						Attributes: line.GetAttributes(),
						Subject: &customersv1.HouseholdRef{
							DisplayName: req.GetPickList().GetCounterparty().GetDisplayName(),
						},
					},
					Details: &customersv1.PreviewConsentRecordRequest_Details{
						Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetPickList().GetLines() {
		if workCtx.Err() != nil {
			break
		}
		select {
		case jobs <- queued:
		case <-workCtx.Done():
		}
	}
	close(jobs)
	wg.Wait()
	stopWork()
	close(failures)
	if err := <-failures; err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/PreviewConsentRecord")
	}
	advanceProductReviewWorkflowRequest := &reviewsv1.AdvanceProductReviewWorkflowRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.AdvanceProductReviewWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	advanceProductReviewWorkflowReply := new(reviewsv1.AdvanceProductReviewWorkflowResponse)
	if err := grpcx.Invoke(ctx, s.conn, reviewsv1.ProductReviewWorkflowService_AdvanceProductReviewWorkflow_FullMethodName, advanceProductReviewWorkflowRequest, advanceProductReviewWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewWorkflowService/AdvanceProductReviewWorkflow")
	}
	onboardingPlan, ok := s.clients.Get("acme.onboarding.v1.OnboardingPlanService").(onboardingv1.OnboardingPlanServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.onboarding.v1.OnboardingPlanService")
	}
	batchGetOnboardingPlansRequest := &onboardingv1.BatchGetOnboardingPlansRequest{Context: req.GetContext()}
	batchGetOnboardingPlansReply, err := onboardingPlan.BatchGetOnboardingPlans(ctx, batchGetOnboardingPlansRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.onboarding.v1.OnboardingPlanService/BatchGetOnboardingPlans")
	}
	batchGetWarehousesRequest := newBatchGetWarehousesRequest(withBatchGetWarehousesContext(req.GetContext()))
	batchGetWarehousesReply, err := s.warehouseClient.BatchGetWarehouses(ctx, batchGetWarehousesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.WarehouseService/BatchGetWarehouses")
	}
	_ = batchGetWarehousesReply
	return &warehousingv1.RecallPickListResponse{
		PickList: &warehousingv1.PickList{
			IssuedAt:   advanceProductReviewWorkflowReply.GetProductReview().GetIssuedAt(),
			PickListId: req.GetPickListId(),
		},
		Outcome: &warehousingv1.RecallPickListResponse_Outcome{
			Message:     batchGetOnboardingPlansReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetPickListApprovalStatus returns a pick list.
func (s *PickListApprovalServer) GetPickListApprovalStatus(ctx context.Context, req *warehousingv1.GetPickListApprovalStatusRequest) (*warehousingv1.GetPickListApprovalStatusResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "pick_list", req.GetPickListId(), "get_pick_list_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get pick list approval status", "tenant", req.GetContext().GetTenantId())
	pauseFulfillmentWorkflowRequest := &fulfillmentv1.PauseFulfillmentWorkflowRequest{
		Context: req.GetContext(),
		Details: &fulfillmentv1.PauseFulfillmentWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	pauseFulfillmentWorkflowReply, err := s.fulfillmentWorkflowPort.PauseFulfillmentWorkflow(ctx, pauseFulfillmentWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentWorkflowService/PauseFulfillmentWorkflow")
	}
	return &warehousingv1.GetPickListApprovalStatusResponse{
		PickList: &warehousingv1.PickList{
			Metadata: pauseFulfillmentWorkflowReply.GetFulfillment().GetMetadata(),
			Audit: &commonv1.AuditInfo{
				CreatedBy: pauseFulfillmentWorkflowReply.GetFulfillment().GetAudit().GetCreatedBy(),
			},
			PickListId: req.GetPickListId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentPickListApproval comments a pick list.
func (s *PickListApprovalServer) CommentPickListApproval(ctx context.Context, req *warehousingv1.CommentPickListApprovalRequest) (*warehousingv1.CommentPickListApprovalResponse, error) {
	if err := s.store.SavePickList(ctx, req.GetContext().GetTenantId(), req.GetPickList()); err != nil {
		return nil, grpcx.Wrap(err, "save pick list")
	}
	s.log.InfoContext(ctx, "comment pick list approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPickList().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "email", req.GetPickList().GetCounterparty().GetEmail())
	rejectRewardRequest := &loyaltyv1.RejectRewardRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Owner: &identityv1.UserRef{Email: tenancy.Value(ctx, "email")},
		},
		Details: &loyaltyv1.RejectRewardRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	rejectRewardReply, err := grpcx.Call(ctx, s.rewardApprovalClient.RejectReward, rejectRewardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardApprovalService/RejectReward")
	}
	return &warehousingv1.CommentPickListApprovalResponse{
		PickList: &warehousingv1.PickList{
			Lines: []*warehousingv1.PickListLine{&warehousingv1.PickListLine{
				Amount: rejectRewardReply.GetReward().GetDiscount().GetAmount(),
			}},
			Audit:        &commonv1.AuditInfo{Etag: rejectRewardReply.GetReward().GetAudit().GetEtag()},
			PickListId:   req.GetPickListId(),
			Counterparty: req.GetPickList().GetCounterparty(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// pickListApprovalServerRecallStorageBinSteps are the ways to run
// acme.warehousing.v1.StorageBinApprovalService/RecallStorageBin, by name.
var pickListApprovalServerRecallStorageBinSteps = map[string]func(*PickListApprovalServer, context.Context, *warehousingv1.RecallStorageBinRequest) (*warehousingv1.RecallStorageBinResponse, error){
	"default": (*PickListApprovalServer).recallStorageBinStep,
	"audited": (*PickListApprovalServer).recallStorageBinAuditedStep,
}

func (s *PickListApprovalServer) recallStorageBinStep(ctx context.Context, request *warehousingv1.RecallStorageBinRequest) (*warehousingv1.RecallStorageBinResponse, error) {
	return s.storageBinApprovalClient.RecallStorageBin(ctx, request)
}

func (s *PickListApprovalServer) recallStorageBinAuditedStep(ctx context.Context, request *warehousingv1.RecallStorageBinRequest) (*warehousingv1.RecallStorageBinResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.warehousing.v1.StorageBinApprovalService/RecallStorageBin")
	return s.storageBinApprovalClient.RecallStorageBin(ctx, request)
}

// validatePutawayTaskSender returns a function that sends one request to
// acme.warehousing.v1.PutawayTaskValidationService/ValidatePutawayTask.
func (s *PickListApprovalServer) validatePutawayTaskSender(request *warehousingv1.ValidatePutawayTaskRequest) func(context.Context) (*warehousingv1.ValidatePutawayTaskResponse, error) {
	return func(ctx context.Context) (*warehousingv1.ValidatePutawayTaskResponse, error) {
		return s.putawayTaskValidationClient.ValidatePutawayTask(ctx, request)
	}
}

// checkPutawayTaskEligibilitySender returns a function that sends one request to
// acme.warehousing.v1.PutawayTaskValidationService/CheckPutawayTaskEligibility.
func (s *PickListApprovalServer) checkPutawayTaskEligibilitySender(request *warehousingv1.CheckPutawayTaskEligibilityRequest) func(context.Context) (*warehousingv1.CheckPutawayTaskEligibilityResponse, error) {
	return func(ctx context.Context) (*warehousingv1.CheckPutawayTaskEligibilityResponse, error) {
		return s.putawayTaskValidationClient.CheckPutawayTaskEligibility(ctx, request)
	}
}
