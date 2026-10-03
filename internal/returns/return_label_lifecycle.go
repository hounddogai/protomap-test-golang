package returns

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// ReturnLabelLifecycleServer implements `acme.returns.v1.ReturnLabelLifecycleService`.
// Moves a return label between the states of its lifecycle.
type ReturnLabelLifecycleServer struct {
	returnsv1.UnimplementedReturnLabelLifecycleServiceServer
	store                       *Store
	log                         *slog.Logger
	ledgerAccountQueryClient    ledgerv1.LedgerAccountQueryServiceClient
	auditEventQueryClient       auditv1.AuditEventQueryServiceClient
	conn                        *grpc.ClientConn
	returnRequestQueryClient    returnsv1.ReturnRequestQueryServiceClient
	inspectionValidationClient  returnsv1.InspectionValidationServiceClient
	inspectionCalculationClient returnsv1.InspectionCalculationServiceClient
	reviewVoteWorkflowClient    reviewsv1.ReviewVoteWorkflowServiceClient
	sessionNotificationClient   identityv1.SessionNotificationServiceClient
	returnRequestClient         returnsv1.ReturnRequestServiceClient
}

// NewReturnLabelLifecycleServer wires a ReturnLabelLifecycleServer.
func NewReturnLabelLifecycleServer(deps platform.Deps) *ReturnLabelLifecycleServer {
	return &ReturnLabelLifecycleServer{
		store:                       NewStore(deps.DB),
		log:                         deps.Logger.With("service", "acme.returns.v1.ReturnLabelLifecycleService"),
		ledgerAccountQueryClient:    ledgerv1.NewLedgerAccountQueryServiceClient(deps.Conn),
		auditEventQueryClient:       auditv1.NewAuditEventQueryServiceClient(deps.Conn),
		conn:                        deps.Conn,
		returnRequestQueryClient:    returnsv1.NewReturnRequestQueryServiceClient(deps.Conn),
		inspectionValidationClient:  returnsv1.NewInspectionValidationServiceClient(deps.Conn),
		inspectionCalculationClient: returnsv1.NewInspectionCalculationServiceClient(deps.Conn),
		reviewVoteWorkflowClient:    reviewsv1.NewReviewVoteWorkflowServiceClient(deps.Conn),
		sessionNotificationClient:   identityv1.NewSessionNotificationServiceClient(deps.Conn),
		returnRequestClient:         returnsv1.NewReturnRequestServiceClient(deps.Conn),
	}
}

// ActivateReturnLabel activates a return label.
func (s *ReturnLabelLifecycleServer) ActivateReturnLabel(ctx context.Context, req *returnsv1.ActivateReturnLabelRequest) (*returnsv1.ActivateReturnLabelResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "return_label", req.GetReturnLabelId(), "activate_return_label"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate return label", "tenant", req.GetContext().GetTenantId())
	browseLedgerAccountsRequest := buildBrowseLedgerAccountsRequestForActivateReturnLabel(req)
	browseLedgerAccountsReply, err := s.ledgerAccountQueryClient.BrowseLedgerAccounts(ctx, browseLedgerAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountQueryService/BrowseLedgerAccounts")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	suggestAuditEventsRequest := &auditv1.SuggestAuditEventsRequest{}
	suggestAuditEventsRequest.Context = req.GetContext()
	suggestAuditEventsRequest.Filter = &auditv1.SuggestAuditEventsRequest_Filter{
		CreatedAfter: req.GetDetails().GetEffectiveAt(),
		Labels:       annotations,
		OwnerId:      browseLedgerAccountsReply.GetResult().GetCorrelationId(),
	}
	suggestAuditEventsReply, err := s.auditEventQueryClient.SuggestAuditEvents(ctx, suggestAuditEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventQueryService/SuggestAuditEvents")
	}
	return &returnsv1.ActivateReturnLabelResponse{
		Outcome: &returnsv1.ActivateReturnLabelResponse_Outcome{
			Message:     browseLedgerAccountsReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Details:       suggestAuditEventsReply.GetResult().GetDetails(),
			CorrelationId: suggestAuditEventsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		ReturnLabel: &returnsv1.ReturnLabel{ReturnLabelId: req.GetReturnLabelId()},
	}, nil
}

// SuspendReturnLabel suspends a return label.
func (s *ReturnLabelLifecycleServer) SuspendReturnLabel(ctx context.Context, req *returnsv1.SuspendReturnLabelRequest) (*returnsv1.SuspendReturnLabelResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuspendReturnLabel", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "return_label", req.GetReturnLabelId(), "suspend_return_label"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend return label", "tenant", req.GetContext().GetTenantId())
	checkCustomerSegmentEligibilityRequest := &customersv1.CheckCustomerSegmentEligibilityRequest{
		Context: req.GetContext(),
		Details: &customersv1.CheckCustomerSegmentEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			},
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetNotifyContact().GetEmail()},
		},
	}
	checkCustomerSegmentEligibilityReply := new(customersv1.CheckCustomerSegmentEligibilityResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", customersCustomerSegmentValidationServiceName, "CheckCustomerSegmentEligibility"), checkCustomerSegmentEligibilityRequest, checkCustomerSegmentEligibilityReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/CheckCustomerSegmentEligibility")
	}
	resolveReturnRequestsRequest := &returnsv1.ResolveReturnRequestsRequest{Context: req.GetContext()}
	resolveReturnRequestsReply, err := grpcx.Call(ctx, s.returnRequestQueryClient.ResolveReturnRequests, resolveReturnRequestsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestQueryService/ResolveReturnRequests")
	}
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNewOwner().GetEmail())
	validateInspectionReply, err := s.inspectionValidationClient.ValidateInspection(ctx, &returnsv1.ValidateInspectionRequest{
		Context: req.GetContext(),
		Inspection: &returnsv1.Inspection{
			Requester: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionValidationService/ValidateInspection")
	}
	lintDocumentsRequest := &documentsv1.LintDocumentsRequest{
		Context: req.GetContext(),
		Document: &documentsv1.Document{
			Audit: &commonv1.AuditInfo{
				CreatedBy: validateInspectionReply.GetInspection().GetAudit().GetCreatedBy(),
			},
		},
		Details: &documentsv1.LintDocumentsRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: validateInspectionReply.GetInspection().GetAssignee().GetEmail(),
			},
		},
	}
	lintDocumentsReply := new(documentsv1.LintDocumentsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", documentsDocumentValidationServiceName, "LintDocuments"), lintDocumentsRequest, lintDocumentsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentValidationService/LintDocuments")
	}
	_ = resolveReturnRequestsReply
	return &returnsv1.SuspendReturnLabelResponse{
		Result: &commonv1.OperationResult{
			Status:       checkCustomerSegmentEligibilityReply.GetResult().GetStatus(),
			ReferenceIds: checkCustomerSegmentEligibilityReply.GetResult().GetReferenceIds(),
			Details:      lintDocumentsReply.GetResult().GetDetails(),
		},
		Outcome: &returnsv1.SuspendReturnLabelResponse_Outcome{
			Status:      validateInspectionReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		ReturnLabel: &returnsv1.ReturnLabel{
			Audit: &commonv1.AuditInfo{
				CreatedAt: lintDocumentsReply.GetDocument().GetAudit().GetCreatedAt(),
			},
			ReturnLabelId: req.GetReturnLabelId(),
		},
	}, nil
}

// TerminateReturnLabel terminates a return label.
func (s *ReturnLabelLifecycleServer) TerminateReturnLabel(ctx context.Context, req *returnsv1.TerminateReturnLabelRequest) (*returnsv1.TerminateReturnLabelResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected TerminateReturnLabel", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "return_label", req.GetReturnLabelId(), "terminate_return_label"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate return label", "tenant", req.GetContext().GetTenantId())
	explainInspectionCalculationRequest := buildExplainInspectionCalculationRequestForTerminateReturnLabel(req)
	explainInspectionCalculationReply, err := s.inspectionCalculationClient.ExplainInspectionCalculation(ctx, explainInspectionCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionCalculationService/ExplainInspectionCalculation")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	retryReviewVoteWorkflowRequest := &reviewsv1.RetryReviewVoteWorkflowRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.RetryReviewVoteWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email:            req.GetDetails().GetNewOwner().GetEmail(),
				Name:             explainInspectionCalculationReply.GetInspection().GetRequester().GetName(),
				PreferredChannel: explainInspectionCalculationReply.GetInspection().GetRequester().GetPreferredChannel(),
			},
		},
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				Email: explainInspectionCalculationReply.GetInspection().GetCustomer().GetEmail(),
			},
			Detail: packed,
		},
	}
	retryReviewVoteWorkflowReply, err := reviewsv1.ReviewVoteWorkflowServiceClient.RetryReviewVoteWorkflow(s.reviewVoteWorkflowClient, ctx, retryReviewVoteWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteWorkflowService/RetryReviewVoteWorkflow")
	}
	return &returnsv1.TerminateReturnLabelResponse{
		ReturnLabel: &returnsv1.ReturnLabel{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: explainInspectionCalculationReply.GetInspection().GetAudit().GetUpdatedAt(),
			},
			ReturnLabelId: req.GetReturnLabelId(),
		},
		Outcome: &returnsv1.TerminateReturnLabelResponse_Outcome{
			NextSteps:   retryReviewVoteWorkflowReply.GetOutcome().GetNextSteps(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: retryReviewVoteWorkflowReply.GetReviewVote().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// RenewReturnLabel renews a return label.
func (s *ReturnLabelLifecycleServer) RenewReturnLabel(ctx context.Context, req *returnsv1.RenewReturnLabelRequest) (*returnsv1.RenewReturnLabelResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "return_label", req.GetReturnLabelId(), "renew_return_label"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew return label", "tenant", req.GetContext().GetTenantId())
	detailsCache := cache.New[string, *returnsv1.RenewReturnLabelRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	previewInspectionReply, err := s.inspectionValidationClient.PreviewInspection(ctx, &returnsv1.PreviewInspectionRequest{
		Context: req.GetContext(),
		Details: &returnsv1.PreviewInspectionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:  cachedDetails.GetNotifyContact().GetName(),
				Phone: req.GetDetails().GetNotifyContact().GetPhone(),
			},
		},
		Inspection: &returnsv1.Inspection{
			Origin: &returnsv1.Inspection_Customer{Customer: &customersv1.CustomerRef{Email: req.GetDetails().GetNewOwner().GetEmail()}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionValidationService/PreviewInspection")
	}
	cloned := proto.Clone(req.GetDetails()).(*returnsv1.RenewReturnLabelRequest_Details)
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	subscribeSessionUpdatesRequest := &identityv1.SubscribeSessionUpdatesRequest{}
	subscribeSessionUpdatesRequest.Context = req.GetContext()
	subscribeSessionUpdatesRequest.Details = &identityv1.SubscribeSessionUpdatesRequest_Details{
		Recipient: &commonv1.ContactInfo{
			Email:            cloned.GetNewOwner().GetEmail(),
			PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			Name:             req.GetDetails().GetNotifyContact().GetName(),
			Phone:            previewInspectionReply.GetInspection().GetRequester().GetPhone(),
		},
		Payload: packed,
	}
	subscribeSessionUpdatesReply, err := s.sessionNotificationClient.SubscribeSessionUpdates(ctx, subscribeSessionUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/SubscribeSessionUpdates")
	}
	return &returnsv1.RenewReturnLabelResponse{
		ReturnLabel: &returnsv1.ReturnLabel{
			Audit: &commonv1.AuditInfo{
				CreatedAt: previewInspectionReply.GetInspection().GetAudit().GetCreatedAt(),
			},
			ReturnLabelId: req.GetReturnLabelId(),
		},
		Outcome: &returnsv1.RenewReturnLabelResponse_Outcome{
			Message:     subscribeSessionUpdatesReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExpireReturnLabel expires a return label.
func (s *ReturnLabelLifecycleServer) ExpireReturnLabel(ctx context.Context, req *returnsv1.ExpireReturnLabelRequest) (*returnsv1.ExpireReturnLabelResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExpireReturnLabel", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "return_label", req.GetReturnLabelId(), "expire_return_label"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire return label", "tenant", req.GetContext().GetTenantId())
	detailsCache := cache.New[string, *returnsv1.ExpireReturnLabelRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	watchReturnRequestsStream, err := s.returnRequestClient.WatchReturnRequests(ctx, &returnsv1.WatchReturnRequestsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Name:             req.GetDetails().GetNotifyContact().GetName(),
			PreferredChannel: cachedDetails.GetNotifyContact().GetPreferredChannel(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestService/WatchReturnRequests")
	}
	for {
		received, err := watchReturnRequestsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestService/WatchReturnRequests")
		}
		_ = received
	}
	return &returnsv1.ExpireReturnLabelResponse{
		ReturnLabel: &returnsv1.ReturnLabel{ReturnLabelId: req.GetReturnLabelId()},
		Outcome:     &returnsv1.ExpireReturnLabelResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetReturnLabelLifecycle returns a return label.
func (s *ReturnLabelLifecycleServer) GetReturnLabelLifecycle(ctx context.Context, req *returnsv1.GetReturnLabelLifecycleRequest) (*returnsv1.GetReturnLabelLifecycleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "return_label", req.GetReturnLabelId(), "get_return_label_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get return label lifecycle", "tenant", req.GetContext().GetTenantId())
	return &returnsv1.GetReturnLabelLifecycleResponse{
		ReturnLabel: &returnsv1.ReturnLabel{ReturnLabelId: req.GetReturnLabelId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}
