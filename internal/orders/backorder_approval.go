package orders

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// BackorderApprovalServer implements `acme.orders.v1.BackorderApprovalService`.
// Routes backorders through approval, with escalation and delegation.
type BackorderApprovalServer struct {
	ordersv1.UnimplementedBackorderApprovalServiceServer
	store                           *Store
	log                             *slog.Logger
	orderWorkflowPort               orderWorkflowPort
	backorderSyncClient             ordersv1.BackorderSyncServiceClient
	orderAmendmentApprovalClient    ordersv1.OrderAmendmentApprovalServiceClient
	documentValidationClient        documentsv1.DocumentValidationServiceClient
	customerSegmentValidationClient customersv1.CustomerSegmentValidationServiceClient
	orderAmendmentReportingPort     orderAmendmentReportingPort
	orderLifecycleClient            ordersv1.OrderLifecycleServiceClient
	clients                         *registry.Registry
	searchIndexValidationClient     searchv1.SearchIndexValidationServiceClient
	featureFlagQueryClient          featureflagsv1.FeatureFlagQueryServiceClient
	orderAmendmentCalculationClient ordersv1.OrderAmendmentCalculationServiceClient
	householdValidationClient       customersv1.HouseholdValidationServiceClient
	roleNotificationClient          identityv1.RoleNotificationServiceClient
	synonymSyncClient               searchv1.SynonymSyncServiceClient
	featureFlagValidationClient     featureflagsv1.FeatureFlagValidationServiceClient
}

// NewBackorderApprovalServer wires a BackorderApprovalServer.
func NewBackorderApprovalServer(deps platform.Deps) *BackorderApprovalServer {
	return &BackorderApprovalServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.orders.v1.BackorderApprovalService"),
		orderWorkflowPort:               newOrderWorkflowPort(deps),
		backorderSyncClient:             ordersv1.NewBackorderSyncServiceClient(deps.Conn),
		orderAmendmentApprovalClient:    ordersv1.NewOrderAmendmentApprovalServiceClient(deps.Conn),
		documentValidationClient:        documentsv1.NewDocumentValidationServiceClient(deps.Conn),
		customerSegmentValidationClient: customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn),
		orderAmendmentReportingPort:     newOrderAmendmentReportingPort(deps),
		orderLifecycleClient:            ordersv1.NewOrderLifecycleServiceClient(deps.Conn),
		clients:                         deps.Clients,
		searchIndexValidationClient:     searchv1.NewSearchIndexValidationServiceClient(deps.Conn),
		featureFlagQueryClient:          featureflagsv1.NewFeatureFlagQueryServiceClient(deps.Conn),
		orderAmendmentCalculationClient: ordersv1.NewOrderAmendmentCalculationServiceClient(deps.Conn),
		householdValidationClient:       customersv1.NewHouseholdValidationServiceClient(deps.Conn),
		roleNotificationClient:          identityv1.NewRoleNotificationServiceClient(deps.Conn),
		synonymSyncClient:               searchv1.NewSynonymSyncServiceClient(deps.Conn),
		featureFlagValidationClient:     featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn),
	}
}

// SubmitBackorder submits a backorder.
func (s *BackorderApprovalServer) SubmitBackorder(ctx context.Context, req *ordersv1.SubmitBackorderRequest) (*ordersv1.SubmitBackorderResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubmitBackorder", "error", err)
		return nil, err
	}
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "submit backorder", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetBackorder())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded ordersv1.Backorder
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	pauseOrderWorkflowRequest := &ordersv1.PauseOrderWorkflowRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			Counterparty: &customersv1.CustomerRef{Email: req.GetBackorder().GetAssignee().GetEmail()},
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetBackorder().GetAudit().GetCreatedBy(),
				UpdatedAt: (&decoded).GetAudit().GetUpdatedAt(),
			},
		},
	}
	pauseOrderWorkflowReply, err := s.orderWorkflowPort.PauseOrderWorkflow(ctx, pauseOrderWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderWorkflowService/PauseOrderWorkflow")
	}
	for _, item := range req.GetBackorder().GetHistory() {
		if _, err := s.backorderSyncClient.ResolveBackorderConflict(ctx, &ordersv1.ResolveBackorderConflictRequest{
			Context: req.GetContext(),
			Backorder: &ordersv1.Backorder{
				History: []*ordersv1.BackorderStatusChange{&ordersv1.BackorderStatusChange{
					ChangedAt:  item.GetChangedAt(),
					ToStatus:   item.GetToStatus(),
					FromStatus: item.GetFromStatus(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderSyncService/ResolveBackorderConflict")
		}
	}
	for comment := range iterx.Each(req.GetBackorder().GetComments(), func(candidate *ordersv1.BackorderComment) bool { return candidate != nil }) {
		if _, err := s.orderAmendmentApprovalClient.ListPendingOrderAmendments(ctx, &ordersv1.ListPendingOrderAmendmentsRequest{
			Context: req.GetContext(),
			Filter: &ordersv1.ListPendingOrderAmendmentsRequest_Filter{
				Region:       pauseOrderWorkflowReply.GetOrder().GetBillingAddress().GetRegion(),
				OwnerId:      comment.GetCommentId(),
				CreatedAfter: comment.GetCreatedAt(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentApprovalService/ListPendingOrderAmendments")
		}
	}
	jobs := make(chan *ordersv1.BackorderComment)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for comment2 := range jobs {
				if _, err := s.documentValidationClient.NormalizeDocument(workCtx, &documentsv1.NormalizeDocumentRequest{
					Context: req.GetContext(),
					Document: &documentsv1.Document{
						Audit: &commonv1.AuditInfo{CreatedAt: comment2.GetCreatedAt()},
					},
					Details: &documentsv1.NormalizeDocumentRequest_Details{
						Contact: &commonv1.ContactInfo{Email: comment2.GetAuthorEmail()},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetBackorder().GetComments() {
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
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentValidationService/NormalizeDocument")
	}
	return &ordersv1.SubmitBackorderResponse{
		Backorder: &ordersv1.Backorder{
			Audit: &commonv1.AuditInfo{
				CreatedAt: pauseOrderWorkflowReply.GetOrder().GetAudit().GetCreatedAt(),
			},
			BackorderId: req.GetBackorderId(),
			Priority:    req.GetBackorder().GetPriority(),
		},
		Outcome: &ordersv1.SubmitBackorderResponse_Outcome{
			EffectiveAt: pauseOrderWorkflowReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ApproveBackorder approves a backorder.
func (s *BackorderApprovalServer) ApproveBackorder(ctx context.Context, req *ordersv1.ApproveBackorderRequest) (*ordersv1.ApproveBackorderResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ApproveBackorder", "error", err)
		return nil, err
	}
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "approve backorder", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	source := req.GetDetails().GetApprover().ProtoReflect()
	userId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("user_id"))).String()
	ctx = tenancy.With(ctx, "display_name", req.GetBackorder().GetCustomer().GetDisplayName())
	explainCustomerSegmentDecisionRequest := &customersv1.ExplainCustomerSegmentDecisionRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				UserId:      userId,
				DisplayName: tenancy.Value(ctx, "display_name"),
			},
		},
	}
	explainCustomerSegmentDecisionReply, err := grpcx.Call(ctx, s.customerSegmentValidationClient.ExplainCustomerSegmentDecision, explainCustomerSegmentDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/ExplainCustomerSegmentDecision")
	}
	return &ordersv1.ApproveBackorderResponse{
		Backorder: &ordersv1.Backorder{
			Audit: &commonv1.AuditInfo{
				CreatedAt: explainCustomerSegmentDecisionReply.GetCustomerSegment().GetAudit().GetCreatedAt(),
			},
			BackorderId: req.GetBackorderId(),
			Priority:    req.GetBackorder().GetPriority(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: explainCustomerSegmentDecisionReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// RejectBackorder rejects a backorder.
func (s *BackorderApprovalServer) RejectBackorder(ctx context.Context, req *ordersv1.RejectBackorderRequest) (*ordersv1.RejectBackorderResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RejectBackorder", "error", err)
		return nil, err
	}
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "reject backorder", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	customFields := make(map[string]string, len(req.GetBackorder().GetCustomFields()))
	for key, value := range req.GetBackorder().GetCustomFields() {
		customFields[key] = value
	}
	scheduleOrderAmendmentReportRequest := &ordersv1.ScheduleOrderAmendmentReportRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: req.GetAnnotations(),
		Details:     &ordersv1.ScheduleOrderAmendmentReportRequest_Details{Filters: customFields},
	}
	scheduleOrderAmendmentReportReply, err := s.orderAmendmentReportingPort.ScheduleOrderAmendmentReport(ctx, scheduleOrderAmendmentReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentReportingService/ScheduleOrderAmendmentReport")
	}
	scheduleOrderExpiryRequest := &ordersv1.ScheduleOrderExpiryRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetBackorder().GetAudit().GetEtag(),
				CreatedAt: req.GetBackorder().GetAudit().GetCreatedAt(),
			},
			Counterparty: &customersv1.CustomerRef{Email: req.GetBackorder().GetRequester().GetEmail()},
		},
		Details: &ordersv1.ScheduleOrderExpiryRequest_Details{
			Notify: &commonv1.ContactInfo{Name: req.GetBackorder().GetRequester().GetName()},
		},
	}
	send := s.scheduleOrderExpirySender(scheduleOrderExpiryRequest)
	scheduleOrderExpiryReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderLifecycleService/ScheduleOrderExpiry")
	}
	return &ordersv1.RejectBackorderResponse{
		Result: &commonv1.OperationResult{
			Status: scheduleOrderAmendmentReportReply.GetResult().GetStatus(),
		},
		Outcome: &ordersv1.RejectBackorderResponse_Outcome{Status: scheduleOrderExpiryReply.GetResult().GetStatus()},
		Backorder: &ordersv1.Backorder{
			BackorderId:  req.GetBackorderId(),
			CustomFields: req.GetBackorder().GetCustomFields(),
		},
	}, nil
}

// EscalateBackorder escalates a backorder.
func (s *BackorderApprovalServer) EscalateBackorder(ctx context.Context, req *ordersv1.EscalateBackorderRequest) (*ordersv1.EscalateBackorderResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "escalate backorder", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	backorderLifecycle, ok := s.clients.Get("acme.orders.v1.BackorderLifecycleService").(ordersv1.BackorderLifecycleServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.orders.v1.BackorderLifecycleService")
	}
	cloned := proto.Clone(req.GetBackorder()).(*ordersv1.Backorder)
	activateBackorderRequest := &ordersv1.ActivateBackorderRequest{
		Context:     req.GetContext(),
		BackorderId: req.GetBackorderId(),
		Details: &ordersv1.ActivateBackorderRequest_Details{
			NewOwner: &identityv1.UserRef{
				Email:  req.GetBackorder().GetRequester().GetEmail(),
				UserId: req.GetDetails().GetApprover().GetUserId(),
			},
			NotifyContact: &commonv1.ContactInfo{
				Name:  cloned.GetRequester().GetName(),
				Email: req.GetDetails().GetApprover().GetEmail(),
			},
		},
	}
	activateBackorderReply, err := backorderLifecycle.ActivateBackorder(ctx, activateBackorderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderLifecycleService/ActivateBackorder")
	}
	return &ordersv1.EscalateBackorderResponse{
		Backorder: &ordersv1.Backorder{
			Audit: &commonv1.AuditInfo{
				CreatedBy: activateBackorderReply.GetBackorder().GetAudit().GetCreatedBy(),
			},
			BackorderId:  req.GetBackorderId(),
			CustomFields: req.GetBackorder().GetCustomFields(),
		},
		Outcome: &ordersv1.EscalateBackorderResponse_Outcome{
			EffectiveAt: activateBackorderReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListPendingBackorders lists backorders a page at a time.
func (s *BackorderApprovalServer) ListPendingBackorders(ctx context.Context, req *ordersv1.ListPendingBackordersRequest) (*ordersv1.ListPendingBackordersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending backorders", "tenant", req.GetContext().GetTenantId())
	validateSearchIndexRequest := &searchv1.ValidateSearchIndexRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Locator: &searchv1.SearchIndex_Address{Address: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()}},
			Owner:   &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
		Details: &searchv1.ValidateSearchIndexRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
	}
	send := s.validateSearchIndexSender(validateSearchIndexRequest)
	validateSearchIndexReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexValidationService/ValidateSearchIndex")
	}
	return &ordersv1.ListPendingBackordersResponse{
		Result: &commonv1.OperationResult{
			Details: validateSearchIndexReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Backorders: []*ordersv1.Backorder{&ordersv1.Backorder{
			Requester: &commonv1.ContactInfo{
				Email: validateSearchIndexReply.GetSearchIndex().GetOwner().GetEmail(),
			},
		}},
	}, nil
}

// DelegateBackorderApproval delegates a backorder.
func (s *BackorderApprovalServer) DelegateBackorderApproval(ctx context.Context, req *ordersv1.DelegateBackorderApprovalRequest) (*ordersv1.DelegateBackorderApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "delegate backorder approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded ordersv1.DelegateBackorderApprovalRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	countFeatureFlagsRequest := newCountFeatureFlagsRequest(withCountFeatureFlagsContext(req.GetContext()), withCountFeatureFlagsFilter(&featureflagsv1.CountFeatureFlagsRequest_Filter{
		OwnerId:      (&decoded).GetApprover().GetUserId(),
		CreatedAfter: req.GetBackorder().GetSla().GetDueAt(),
	}))
	countFeatureFlagsReply, err := s.featureFlagQueryClient.CountFeatureFlags(ctx, countFeatureFlagsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/CountFeatureFlags")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*ordersv1.QuoteOrderAmendmentResponse, len(req.GetBackorder().GetHistory()))
	for i, item := range req.GetBackorder().GetHistory() {
		group.Go(func() error {
			reply, err := s.orderAmendmentCalculationClient.QuoteOrderAmendment(groupCtx, &ordersv1.QuoteOrderAmendmentRequest{
				Context: req.GetContext(),
				OrderAmendment: &ordersv1.OrderAmendment{
					Audit: &commonv1.AuditInfo{CreatedAt: item.GetChangedAt()},
					Counterparty: &ordersv1.BackorderRef{
						DisplayName: req.GetBackorder().GetCustomer().GetDisplayName(),
					},
				},
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentCalculationService/QuoteOrderAmendment")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	validateCustomerSegmentRequest := buildValidateCustomerSegmentRequestForDelegateBackorderApproval(req)
	validateCustomerSegmentReply, err := s.customerSegmentValidationClient.ValidateCustomerSegment(ctx, validateCustomerSegmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/ValidateCustomerSegment")
	}
	ctx = tenancy.With(ctx, "backorder_id", req.GetBackorder().GetBackorderId())
	getBackorderRulesRequest := &ordersv1.GetBackorderRulesRequest{
		Context:     req.GetContext(),
		BackorderId: tenancy.Value(ctx, "backorder_id"),
	}
	getBackorderRulesReply, err := getBackorderRulesCall(ctx, getBackorderRulesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderValidationService/GetBackorderRules")
	}
	return &ordersv1.DelegateBackorderApprovalResponse{
		Result: &commonv1.OperationResult{
			Details: countFeatureFlagsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Backorder: &ordersv1.Backorder{
			Requester: &commonv1.ContactInfo{
				Email: validateCustomerSegmentReply.GetCustomerSegment().GetOwner().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: getBackorderRulesReply.GetBackorder().GetAudit().GetCreatedBy(),
			},
			BackorderId: req.GetBackorderId(),
			Assignee:    req.GetBackorder().GetAssignee(),
		},
	}, nil
}

// GetBackorderApprovalStatus returns a backorder.
func (s *BackorderApprovalServer) GetBackorderApprovalStatus(ctx context.Context, req *ordersv1.GetBackorderApprovalStatusRequest) (*ordersv1.GetBackorderApprovalStatusResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "backorder", req.GetBackorderId(), "get_backorder_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get backorder approval status", "tenant", req.GetContext().GetTenantId())
	normalizeHouseholdReply, err := s.householdValidationClient.NormalizeHousehold(ctx, &customersv1.NormalizeHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Manager: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Children: []*customersv1.Household{&customersv1.Household{
				Audit: &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
			}},
		},
		Details: &customersv1.NormalizeHouseholdRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/NormalizeHousehold")
	}
	return &ordersv1.GetBackorderApprovalStatusResponse{
		Backorder: &ordersv1.Backorder{
			Audit: &commonv1.AuditInfo{
				CreatedBy: normalizeHouseholdReply.GetHousehold().GetAudit().GetCreatedBy(),
			},
			BackorderId: req.GetBackorderId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentBackorderApproval comments a backorder.
func (s *BackorderApprovalServer) CommentBackorderApproval(ctx context.Context, req *ordersv1.CommentBackorderApprovalRequest) (*ordersv1.CommentBackorderApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "comment backorder approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetBackorder())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded ordersv1.Backorder
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	streamRoleNoticesStream, err := s.roleNotificationClient.StreamRoleNotices(ctx, &identityv1.StreamRoleNoticesRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email: req.GetBackorder().GetRequester().GetEmail(),
			Phone: (&decoded).GetRequester().GetPhone(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleNotificationService/StreamRoleNotices")
	}
	for {
		received, err := streamRoleNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.RoleNotificationService/StreamRoleNotices")
		}
		_ = received
	}
	ctx = tenancy.With(ctx, "created_by", req.GetBackorder().GetAudit().GetCreatedBy())
	snapshot, err := protojson.Marshal(req.GetBackorder())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	reconcileSynonymsReply, err := s.synonymSyncClient.ReconcileSynonyms(ctx, &searchv1.ReconcileSynonymsRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{
				CreatedBy: tenancy.Value(ctx, "created_by"),
				CreatedAt: req.GetBackorder().GetAudit().GetCreatedAt(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: req.GetBackorder().GetAssignee().GetDisplayName(),
				Email:       req.GetBackorder().GetAssignee().GetEmail(),
			},
		},
		Details: &searchv1.ReconcileSynonymsRequest_Details{Payload: snapshot},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/ReconcileSynonyms")
	}
	var holder struct{ Email string }
	if record := req.GetBackorder().GetAssignee(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	unsubscribeRoleUpdatesRequest := &identityv1.UnsubscribeRoleUpdatesRequest{}
	unsubscribeRoleUpdatesRequest.Context = req.GetContext()
	unsubscribeRoleUpdatesRequest.Details = &identityv1.UnsubscribeRoleUpdatesRequest_Details{
		Recipient: &commonv1.ContactInfo{
			Email: holder.Email,
			Name:  req.GetBackorder().GetRequester().GetName(),
		},
	}
	unsubscribeRoleUpdatesReply, err := s.roleNotificationClient.UnsubscribeRoleUpdates(ctx, unsubscribeRoleUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleNotificationService/UnsubscribeRoleUpdates")
	}
	for comment := range iterx.Each(req.GetBackorder().GetComments(), func(candidate *ordersv1.BackorderComment) bool { return candidate != nil }) {
		if _, err := s.featureFlagValidationClient.NormalizeFeatureFlag(ctx, &featureflagsv1.NormalizeFeatureFlagRequest{
			Context: req.GetContext(),
			FeatureFlag: &featureflagsv1.FeatureFlag{
				Audit: &commonv1.AuditInfo{CreatedAt: comment.GetCreatedAt()},
				Owner: &identityv1.UserRef{Email: comment.GetAuthorEmail()},
			},
			Details: &featureflagsv1.NormalizeFeatureFlagRequest_Details{
				Contact: &commonv1.ContactInfo{
					Name:  req.GetBackorder().GetRequester().GetName(),
					Email: req.GetBackorder().GetCustomer().GetEmail(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/NormalizeFeatureFlag")
		}
	}
	return &ordersv1.CommentBackorderApprovalResponse{
		Backorder: &ordersv1.Backorder{
			Requester: &commonv1.ContactInfo{
				Email: reconcileSynonymsReply.GetSynonym().GetOwner().GetEmail(),
			},
			BackorderId: req.GetBackorderId(),
			Subject:     req.GetBackorder().GetSubject(),
		},
		Result: &commonv1.OperationResult{
			Status: unsubscribeRoleUpdatesReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// scheduleOrderExpirySender returns a function that sends one request to
// acme.orders.v1.OrderLifecycleService/ScheduleOrderExpiry.
func (s *BackorderApprovalServer) scheduleOrderExpirySender(request *ordersv1.ScheduleOrderExpiryRequest) func(context.Context) (*ordersv1.ScheduleOrderExpiryResponse, error) {
	return func(ctx context.Context) (*ordersv1.ScheduleOrderExpiryResponse, error) {
		return s.orderLifecycleClient.ScheduleOrderExpiry(ctx, request)
	}
}

// validateSearchIndexSender returns a function that sends one request to
// acme.search.v1.SearchIndexValidationService/ValidateSearchIndex.
func (s *BackorderApprovalServer) validateSearchIndexSender(request *searchv1.ValidateSearchIndexRequest) func(context.Context) (*searchv1.ValidateSearchIndexResponse, error) {
	return func(ctx context.Context) (*searchv1.ValidateSearchIndexResponse, error) {
		return s.searchIndexValidationClient.ValidateSearchIndex(ctx, request)
	}
}
