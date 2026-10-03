package returns

import (
	"context"
	"errors"
	"io"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// ReturnRequestWorkflowServer implements `acme.returns.v1.ReturnRequestWorkflowService`.
// Runs the long-lived workflow that takes a return request from start to finish.
type ReturnRequestWorkflowServer struct {
	returnsv1.UnsafeReturnRequestWorkflowServiceServer
	store                          *Store
	log                            *slog.Logger
	returnLabelApprovalClient      returnsv1.ReturnLabelApprovalServiceClient
	accessPolicyQueryClient        authorizationv1.AccessPolicyQueryServiceClient
	sessionQueryClient             identityv1.SessionQueryServiceClient
	conn                           *grpc.ClientConn
	returnLabelCalculationClient   returnsv1.ReturnLabelCalculationServiceClient
	bus                            *eventbus.Bus
	returnLabelValidationClient    returnsv1.ReturnLabelValidationServiceClient
	returnRequestClient            returnsv1.ReturnRequestServiceClient
	consentRecordCalculationClient customersv1.ConsentRecordCalculationServiceClient
}

// NewReturnRequestWorkflowServer wires a ReturnRequestWorkflowServer.
func NewReturnRequestWorkflowServer(deps platform.Deps) *ReturnRequestWorkflowServer {
	return &ReturnRequestWorkflowServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.returns.v1.ReturnRequestWorkflowService"),
		returnLabelApprovalClient:      returnsv1.NewReturnLabelApprovalServiceClient(deps.Conn),
		accessPolicyQueryClient:        authorizationv1.NewAccessPolicyQueryServiceClient(deps.Conn),
		sessionQueryClient:             identityv1.NewSessionQueryServiceClient(deps.Conn),
		conn:                           deps.Conn,
		returnLabelCalculationClient:   returnsv1.NewReturnLabelCalculationServiceClient(deps.Conn),
		bus:                            deps.Bus,
		returnLabelValidationClient:    returnsv1.NewReturnLabelValidationServiceClient(deps.Conn),
		returnRequestClient:            returnsv1.NewReturnRequestServiceClient(deps.Conn),
		consentRecordCalculationClient: customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
	}
}

// StartReturnRequestWorkflow starts a return request.
func (s *ReturnRequestWorkflowServer) StartReturnRequestWorkflow(ctx context.Context, req *returnsv1.StartReturnRequestWorkflowRequest) (*returnsv1.StartReturnRequestWorkflowResponse, error) {
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "start return request workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	recallReturnLabelRequest := buildRecallReturnLabelRequestForStartReturnRequestWorkflow(req)
	recallReturnLabelReply, err := s.returnLabelApprovalClient.RecallReturnLabel(ctx, recallReturnLabelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelApprovalService/RecallReturnLabel")
	}
	findAccessPoliciesReply, err := s.accessPolicyQueryClient.FindAccessPolicies(ctx, &authorizationv1.FindAccessPoliciesRequest{
		Context: req.GetContext(),
		Filter: &authorizationv1.FindAccessPoliciesRequest_Filter{
			Region:       req.GetReturnRequest().GetBillingAddress().GetRegion(),
			CreatedAfter: recallReturnLabelReply.GetReturnLabel().GetAudit().GetCreatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyQueryService/FindAccessPolicies")
	}
	return &returnsv1.StartReturnRequestWorkflowResponse{
		ReturnRequest: &returnsv1.ReturnRequest{
			Metadata:        recallReturnLabelReply.GetReturnLabel().GetMetadata(),
			ReturnRequestId: req.GetReturnRequestId(),
			DueAt:           req.GetReturnRequest().GetDueAt(),
		},
		Result: &commonv1.OperationResult{
			Message: findAccessPoliciesReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// AdvanceReturnRequestWorkflow advances a return request.
func (s *ReturnRequestWorkflowServer) AdvanceReturnRequestWorkflow(ctx context.Context, req *returnsv1.AdvanceReturnRequestWorkflowRequest) (*returnsv1.AdvanceReturnRequestWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "advance return request workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeSessionsRequest := &identityv1.SummarizeSessionsRequest{}
	summarizeSessionsRequest.Context = req.GetContext()
	summarizeSessionsRequest.Annotations = annotations
	summarizeSessionsReply, err := s.sessionQueryClient.SummarizeSessions(ctx, summarizeSessionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/SummarizeSessions")
	}
	cloned := proto.Clone(req.GetReturnRequest()).(*returnsv1.ReturnRequest)
	packed, err := anypb.New(req.GetReturnRequest())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	delegateReviewVoteApprovalRequest := &reviewsv1.DelegateReviewVoteApprovalRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{DisplayName: cloned.GetCounterparty().GetDisplayName()},
			Detail:  packed,
		},
	}
	delegateReviewVoteApprovalReply := new(reviewsv1.DelegateReviewVoteApprovalResponse)
	if err := grpcx.Invoke(ctx, s.conn, reviewsv1.ReviewVoteApprovalService_DelegateReviewVoteApproval_FullMethodName, delegateReviewVoteApprovalRequest, delegateReviewVoteApprovalReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteApprovalService/DelegateReviewVoteApproval")
	}
	return &returnsv1.AdvanceReturnRequestWorkflowResponse{
		Outcome: &returnsv1.AdvanceReturnRequestWorkflowResponse_Outcome{
			Message:     summarizeSessionsReply.GetResult().GetMessage(),
			EffectiveAt: delegateReviewVoteApprovalReply.GetOutcome().GetEffectiveAt(),
		},
		ReturnRequest: &returnsv1.ReturnRequest{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: delegateReviewVoteApprovalReply.GetReviewVote().GetSubject().GetDisplayName(),
			},
			ReturnRequestId: req.GetReturnRequestId(),
			Status:          req.GetReturnRequest().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CancelReturnRequestWorkflow cancels a return request.
func (s *ReturnRequestWorkflowServer) CancelReturnRequestWorkflow(ctx context.Context, req *returnsv1.CancelReturnRequestWorkflowRequest) (*returnsv1.CancelReturnRequestWorkflowResponse, error) {
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "cancel return request workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	var iban string
	switch choice := req.GetReturnRequest().GetSettlement().(type) {
	case *returnsv1.ReturnRequest_Card:
		iban = choice.Card.GetHolderName()
	case *returnsv1.ReturnRequest_Bank:
		iban = choice.Bank.GetIban()
	case *returnsv1.ReturnRequest_VoucherCode:
		iban = choice.VoucherCode
	}
	recalculateReturnLabelsRequest := newRecalculateReturnLabelsRequest(withRecalculateReturnLabelsContext(req.GetContext()), withRecalculateReturnLabelsReturnLabel(&returnsv1.ReturnLabel{
		Settlement: &returnsv1.ReturnLabel_Bank{Bank: &returnsv1.ReturnLabelBankSettlement{Iban: iban}},
		Audit: &commonv1.AuditInfo{
			Etag:      req.GetReturnRequest().GetAudit().GetEtag(),
			UpdatedAt: req.GetReturnRequest().GetAudit().GetUpdatedAt(),
			UpdatedBy: req.GetReturnRequest().GetAudit().GetUpdatedBy(),
		},
	}))
	recalculateReturnLabelsReply, err := s.returnLabelCalculationClient.RecalculateReturnLabels(ctx, recalculateReturnLabelsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/RecalculateReturnLabels")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "returns.cancel_return_request_workflow.explain_return_label_decision",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"return_label.billing_address.country_code": recalculateReturnLabelsReply.GetReturnLabel().GetBillingAddress().GetCountryCode(),
			"context.tenant_id":                         req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	for _, attachment := range req.GetReturnRequest().GetAttachments() {
		if _, err := s.returnLabelValidationClient.NormalizeReturnLabel(ctx, &returnsv1.NormalizeReturnLabelRequest{
			Context: req.GetContext(),
			ReturnLabel: &returnsv1.ReturnLabel{
				Attachments: []*returnsv1.ReturnLabelAttachment{&returnsv1.ReturnLabelAttachment{
					SizeBytes: attachment.GetSizeBytes(),
					Uri:       attachment.GetUri(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelValidationService/NormalizeReturnLabel")
		}
	}
	return &returnsv1.CancelReturnRequestWorkflowResponse{
		ReturnRequest: &returnsv1.ReturnRequest{
			BillingAddress:  recalculateReturnLabelsReply.GetReturnLabel().GetBillingAddress(),
			ReturnRequestId: req.GetReturnRequestId(),
			Metadata:        req.GetReturnRequest().GetMetadata(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// InspectReturnRequestWorkflow returns a return request.
func (s *ReturnRequestWorkflowServer) InspectReturnRequestWorkflow(ctx context.Context, req *returnsv1.InspectReturnRequestWorkflowRequest) (*returnsv1.InspectReturnRequestWorkflowResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "return_request", req.GetReturnRequestId(), "inspect_return_request_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect return request workflow", "tenant", req.GetContext().GetTenantId())
	countSessionsRequest := &identityv1.CountSessionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.CountSessionsRequest_Filter{
			OwnerId:      req.GetReturnRequestId(),
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	}
	countSessionsReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*identityv1.CountSessionsResponse, error) {
		return s.sessionQueryClient.CountSessions(ctx, countSessionsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/CountSessions")
	}
	watchReturnRequestsStream, err := s.returnRequestClient.WatchReturnRequests(ctx, &returnsv1.WatchReturnRequestsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		Filter: &returnsv1.WatchReturnRequestsRequest_Filter{
			Labels:       countSessionsReply.GetResult().GetDetails(),
			CreatedAfter: req.GetContext().GetDeadline(),
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
	return &returnsv1.InspectReturnRequestWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message:       countSessionsReply.GetResult().GetMessage(),
			CorrelationId: countSessionsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		ReturnRequest: &returnsv1.ReturnRequest{ReturnRequestId: req.GetReturnRequestId()},
	}, nil
}

// ListReturnRequestWorkflows lists return requests a page at a time.
func (s *ReturnRequestWorkflowServer) ListReturnRequestWorkflows(ctx context.Context, req *returnsv1.ListReturnRequestWorkflowsRequest) (*returnsv1.ListReturnRequestWorkflowsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list return request workflows", "tenant", req.GetContext().GetTenantId())
	return &returnsv1.ListReturnRequestWorkflowsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SignalReturnRequestWorkflow signals a return request.
func (s *ReturnRequestWorkflowServer) SignalReturnRequestWorkflow(ctx context.Context, req *returnsv1.SignalReturnRequestWorkflowRequest) (*returnsv1.SignalReturnRequestWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReturnRequest(ctx, req.GetContext().GetTenantId(), req.GetReturnRequest()); err != nil {
		return nil, grpcx.Wrap(err, "save return request")
	}
	s.log.InfoContext(ctx, "signal return request workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnRequest().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetReturnRequest())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	quoteConsentRecordReply, err := s.consentRecordCalculationClient.QuoteConsentRecord(ctx, &customersv1.QuoteConsentRecordRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetReturnRequest().GetCounterparty().GetDisplayName(),
			},
			Detail: packed,
		},
		Details: &customersv1.QuoteConsentRecordRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region:   req.GetReturnRequest().GetBillingAddress().GetRegion(),
				Locality: req.GetReturnRequest().GetBillingAddress().GetLocality(),
				Lines:    req.GetReturnRequest().GetBillingAddress().GetLines(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/QuoteConsentRecord")
	}
	return &returnsv1.SignalReturnRequestWorkflowResponse{
		ReturnRequest: &returnsv1.ReturnRequest{
			Total:           quoteConsentRecordReply.GetBreakdown().GetTotal(),
			ReturnRequestId: req.GetReturnRequestId(),
			Counterparty:    req.GetReturnRequest().GetCounterparty(),
		},
		Result: &commonv1.OperationResult{Status: quoteConsentRecordReply.GetResult().GetStatus()},
	}, nil
}
