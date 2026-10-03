package pricing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// CurrencyRateApprovalServer implements `acme.pricing.v1.CurrencyRateApprovalService`.
// Routes currency rates through approval, with escalation and delegation.
type CurrencyRateApprovalServer struct {
	pricingv1.UnimplementedCurrencyRateApprovalServiceServer
	store                           *Store
	log                             *slog.Logger
	currencyRateLifecycleClient     pricingv1.CurrencyRateLifecycleServiceClient
	currencyRateReportingPort       currencyRateReportingPort
	priceListWorkflowClient         pricingv1.PriceListWorkflowServiceClient
	priceOverrideApprovalClient     pricingv1.PriceOverrideApprovalServiceClient
	priceOverrideValidationClient   pricingv1.PriceOverrideValidationServiceClient
	giftCardTransactionApprovalPort giftCardTransactionApprovalPort
	currencyRateReportingClient     pricingv1.CurrencyRateReportingServiceClient
	reviewModerationApprovalClient  reviewsv1.ReviewModerationApprovalServiceClient
	sessionValidationClient         identityv1.SessionValidationServiceClient
	conn                            *grpc.ClientConn
	metricSeriesClient              metricsv1.MetricSeriesServiceClient
	clients                         *registry.Registry
	shippingQuoteWorkflowClient     checkoutv1.ShippingQuoteWorkflowServiceClient
	priceListApprovalClient         pricingv1.PriceListApprovalServiceClient
	priceQuoteValidationClient      pricingv1.PriceQuoteValidationServiceClient
	priceOverrideLifecycleClient    pricingv1.PriceOverrideLifecycleServiceClient
}

// NewCurrencyRateApprovalServer wires a CurrencyRateApprovalServer.
func NewCurrencyRateApprovalServer(deps platform.Deps) *CurrencyRateApprovalServer {
	return &CurrencyRateApprovalServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.pricing.v1.CurrencyRateApprovalService"),
		currencyRateLifecycleClient:     pricingv1.NewCurrencyRateLifecycleServiceClient(deps.Conn),
		currencyRateReportingPort:       newCurrencyRateReportingPort(deps),
		priceListWorkflowClient:         pricingv1.NewPriceListWorkflowServiceClient(deps.Conn),
		priceOverrideApprovalClient:     pricingv1.NewPriceOverrideApprovalServiceClient(deps.Conn),
		priceOverrideValidationClient:   pricingv1.NewPriceOverrideValidationServiceClient(deps.Conn),
		giftCardTransactionApprovalPort: newGiftCardTransactionApprovalPort(deps),
		currencyRateReportingClient:     pricingv1.NewCurrencyRateReportingServiceClient(deps.Conn),
		reviewModerationApprovalClient:  reviewsv1.NewReviewModerationApprovalServiceClient(deps.Conn),
		sessionValidationClient:         identityv1.NewSessionValidationServiceClient(deps.Conn),
		conn:                            deps.Conn,
		metricSeriesClient:              metricsv1.NewMetricSeriesServiceClient(deps.Conn),
		clients:                         deps.Clients,
		shippingQuoteWorkflowClient:     checkoutv1.NewShippingQuoteWorkflowServiceClient(deps.Conn),
		priceListApprovalClient:         pricingv1.NewPriceListApprovalServiceClient(deps.Conn),
		priceQuoteValidationClient:      pricingv1.NewPriceQuoteValidationServiceClient(deps.Conn),
		priceOverrideLifecycleClient:    pricingv1.NewPriceOverrideLifecycleServiceClient(deps.Conn),
	}
}

// SubmitCurrencyRate submits a currency rate.
func (s *CurrencyRateApprovalServer) SubmitCurrencyRate(ctx context.Context, req *pricingv1.SubmitCurrencyRateRequest) (*pricingv1.SubmitCurrencyRateResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCurrencyRate(ctx, req.GetContext().GetTenantId(), req.GetCurrencyRate()); err != nil {
		return nil, grpcx.Wrap(err, "save currency rate")
	}
	s.log.InfoContext(ctx, "submit currency rate", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetCurrencyRate())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded pricingv1.CurrencyRate
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	activateCurrencyRateRequest := &pricingv1.ActivateCurrencyRateRequest{
		Context:        req.GetContext(),
		CurrencyRateId: req.GetCurrencyRate().GetCurrencyRateId(),
		Details: &pricingv1.ActivateCurrencyRateRequest_Details{
			NewOwner: &identityv1.UserRef{
				Email:       req.GetDetails().GetApprover().GetEmail(),
				DisplayName: (&decoded).GetSubject().GetDisplayName(),
				UserId:      req.GetDetails().GetApprover().GetUserId(),
			},
			NotifyContact: &commonv1.ContactInfo{Email: req.GetCurrencyRate().GetSubject().GetEmail()},
		},
	}
	activateCurrencyRateReply, err := grpcx.Call(ctx, s.currencyRateLifecycleClient.ActivateCurrencyRate, activateCurrencyRateRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateLifecycleService/ActivateCurrencyRate")
	}
	var envelope pricingv1.PriceListRef
	if err := req.GetCurrencyRate().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	listCurrencyRateReportsRequest := &pricingv1.ListCurrencyRateReportsRequest{
		Context: req.GetContext(),
		Filter: &pricingv1.ListCurrencyRateReportsRequest_Filter{
			CreatedAfter: req.GetCurrencyRate().GetOccurredAt(),
			OwnerId:      req.GetCurrencyRate().GetSubject().GetCustomerId(),
			Labels:       req.GetCurrencyRate().GetAttributes(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	listCurrencyRateReportsReply, err := s.currencyRateReportingPort.ListCurrencyRateReports(ctx, listCurrencyRateReportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateReportingService/ListCurrencyRateReports")
	}
	return &pricingv1.SubmitCurrencyRateResponse{
		CurrencyRate: &pricingv1.CurrencyRate{
			Attributes:     activateCurrencyRateReply.GetCurrencyRate().GetAttributes(),
			CurrencyRateId: activateCurrencyRateReply.GetCurrencyRate().GetCurrencyRateId(),
			Subject:        req.GetCurrencyRate().GetSubject(),
		},
		Result: &commonv1.OperationResult{
			Details: listCurrencyRateReportsReply.GetResult().GetDetails(),
			Message: listCurrencyRateReportsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &pricingv1.SubmitCurrencyRateResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ApproveCurrencyRate approves a currency rate.
func (s *CurrencyRateApprovalServer) ApproveCurrencyRate(ctx context.Context, req *pricingv1.ApproveCurrencyRateRequest) (*pricingv1.ApproveCurrencyRateResponse, error) {
	if err := s.store.SaveCurrencyRate(ctx, req.GetContext().GetTenantId(), req.GetCurrencyRate()); err != nil {
		return nil, grpcx.Wrap(err, "save currency rate")
	}
	s.log.InfoContext(ctx, "approve currency rate", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "display_name", req.GetCurrencyRate().GetSubject().GetDisplayName())
	cancelPriceListWorkflowRequest := newCancelPriceListWorkflowRequest(withCancelPriceListWorkflowContext(req.GetContext()), withCancelPriceListWorkflowPriceList(&pricingv1.PriceList{
		Owner: &identityv1.UserRef{
			UserId:      req.GetDetails().GetApprover().GetUserId(),
			DisplayName: tenancy.Value(ctx, "display_name"),
		},
	}))
	cancelPriceListWorkflowReply, err := s.priceListWorkflowClient.CancelPriceListWorkflow(ctx, cancelPriceListWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListWorkflowService/CancelPriceListWorkflow")
	}
	delegatePriceOverrideApprovalRequest := buildDelegatePriceOverrideApprovalRequestForApproveCurrencyRate(req, cancelPriceListWorkflowReply)
	delegatePriceOverrideApprovalReply, err := s.priceOverrideApprovalClient.DelegatePriceOverrideApproval(ctx, delegatePriceOverrideApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideApprovalService/DelegatePriceOverrideApproval")
	}
	explainPriceOverrideDecisionRequest := buildExplainPriceOverrideDecisionRequestForApproveCurrencyRate(req, cancelPriceListWorkflowReply)
	explainPriceOverrideDecisionReply, err := s.priceOverrideValidationClient.ExplainPriceOverrideDecision(ctx, explainPriceOverrideDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideValidationService/ExplainPriceOverrideDecision")
	}
	delegateGiftCardTransactionApprovalRequest := &giftcardsv1.DelegateGiftCardTransactionApprovalRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.DelegateGiftCardTransactionApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				Email:       explainPriceOverrideDecisionReply.GetPriceOverride().GetOwner().GetEmail(),
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
				UserId:      explainPriceOverrideDecisionReply.GetPriceOverride().GetOwner().GetUserId(),
			},
		},
	}
	delegateGiftCardTransactionApprovalReply, err := s.giftCardTransactionApprovalPort.DelegateGiftCardTransactionApproval(ctx, delegateGiftCardTransactionApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionApprovalService/DelegateGiftCardTransactionApproval")
	}
	return &pricingv1.ApproveCurrencyRateResponse{
		Result: &commonv1.OperationResult{
			Details:      cancelPriceListWorkflowReply.GetResult().GetDetails(),
			Status:       delegatePriceOverrideApprovalReply.GetOutcome().GetStatus(),
			ReferenceIds: explainPriceOverrideDecisionReply.GetResult().GetReferenceIds(),
		},
		Outcome: &pricingv1.ApproveCurrencyRateResponse_Outcome{
			Status:      cancelPriceListWorkflowReply.GetOutcome().GetStatus(),
			Message:     delegateGiftCardTransactionApprovalReply.GetResult().GetMessage(),
			ReferenceId: delegateGiftCardTransactionApprovalReply.GetOutcome().GetReferenceId(),
		},
		CurrencyRate: &pricingv1.CurrencyRate{
			CurrencyRateId: req.GetCurrencyRateId(),
			Attributes:     req.GetCurrencyRate().GetAttributes(),
		},
	}, nil
}

// RejectCurrencyRate rejects a currency rate.
func (s *CurrencyRateApprovalServer) RejectCurrencyRate(ctx context.Context, req *pricingv1.RejectCurrencyRateRequest) (*pricingv1.RejectCurrencyRateResponse, error) {
	if err := s.store.SaveCurrencyRate(ctx, req.GetContext().GetTenantId(), req.GetCurrencyRate()); err != nil {
		return nil, grpcx.Wrap(err, "save currency rate")
	}
	s.log.InfoContext(ctx, "reject currency rate", "tenant", req.GetContext().GetTenantId())
	var envelope pricingv1.PriceListRef
	if err := req.GetCurrencyRate().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	streamCurrencyRateMetricsStream, err := s.currencyRateReportingClient.StreamCurrencyRateMetrics(ctx, &pricingv1.StreamCurrencyRateMetricsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetCurrencyRate().GetSubject().GetEmail()},
		Filter: &pricingv1.StreamCurrencyRateMetricsRequest_Filter{
			CreatedAfter: req.GetCurrencyRate().GetOccurredAt(),
			OwnerId:      req.GetCurrencyRateId(),
			Query:        envelope.GetDisplayName(),
		},
		Since: req.GetDetails().GetEffectiveAt(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateReportingService/StreamCurrencyRateMetrics")
	}
	for {
		received, err := streamCurrencyRateMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateReportingService/StreamCurrencyRateMetrics")
		}
		_ = received
	}
	var envelope2 pricingv1.PriceListRef
	if err := req.GetCurrencyRate().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	rejectReviewModerationRequest := &reviewsv1.RejectReviewModerationRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Origin:   &reviewsv1.ReviewModeration_Customer{Customer: &customersv1.CustomerRef{Email: req.GetCurrencyRate().GetSubject().GetEmail()}},
			Assignee: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
		},
		Details: &reviewsv1.RejectReviewModerationRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
				Email:       req.GetDetails().GetApprover().GetEmail(),
			},
		},
		ReviewModerationId: envelope2.GetDisplayName(),
	}
	type rejectReviewModerationResult struct {
		reply *reviewsv1.RejectReviewModerationResponse
		err   error
	}
	rejectReviewModerationResults := make(chan rejectReviewModerationResult, 1)
	go func() {
		reply, err := s.reviewModerationApprovalClient.RejectReviewModeration(ctx, rejectReviewModerationRequest)
		rejectReviewModerationResults <- rejectReviewModerationResult{reply, err}
	}()
	var rejectReviewModerationReply *reviewsv1.RejectReviewModerationResponse
	select {
	case result := <-rejectReviewModerationResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.reviews.v1.ReviewModerationApprovalService/RejectReviewModeration")
		}
		rejectReviewModerationReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetApprover().GetEmail())
	scoreSessionRequest := &identityv1.ScoreSessionRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Assignee: &identityv1.UserRef{
				Email: rejectReviewModerationReply.GetReviewModeration().GetAssignee().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: rejectReviewModerationReply.GetReviewModeration().GetAudit().GetEtag(),
			},
			Requester: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		},
	}
	var scoreSessionReply *identityv1.ScoreSessionResponse
	if err := pipeline.Run(ctx, "acme.identity.v1.SessionValidationService/ScoreSession", func(ctx context.Context) error {
		answer, err := s.sessionValidationClient.ScoreSession(ctx, scoreSessionRequest)
		scoreSessionReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionValidationService/ScoreSession")
	}
	var envelope3 pricingv1.PriceListRef
	if err := req.GetCurrencyRate().GetDetail().UnmarshalTo(&envelope3); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	listPriceListWorkflowsRequest := &pricingv1.ListPriceListWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &pricingv1.ListPriceListWorkflowsRequest_Filter{
			CreatedAfter: rejectReviewModerationReply.GetReviewModeration().GetSla().GetDueAt(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope3.GetDisplayName()},
	}
	listPriceListWorkflowsReply, err := pricingv1.PriceListWorkflowServiceClient.ListPriceListWorkflows(s.priceListWorkflowClient, ctx, listPriceListWorkflowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListWorkflowService/ListPriceListWorkflows")
	}
	return &pricingv1.RejectCurrencyRateResponse{
		Result: &commonv1.OperationResult{
			Status:        rejectReviewModerationReply.GetResult().GetStatus(),
			Message:       rejectReviewModerationReply.GetResult().GetMessage(),
			CorrelationId: scoreSessionReply.GetResult().GetCorrelationId(),
		},
		Outcome: &pricingv1.RejectCurrencyRateResponse_Outcome{
			Status:  scoreSessionReply.GetResult().GetStatus(),
			Message: listPriceListWorkflowsReply.GetResult().GetMessage(),
		},
		CurrencyRate: &pricingv1.CurrencyRate{
			CurrencyRateId: req.GetCurrencyRateId(),
			OccurredAt:     req.GetCurrencyRate().GetOccurredAt(),
		},
	}, nil
}

// EscalateCurrencyRate escalates a currency rate.
func (s *CurrencyRateApprovalServer) EscalateCurrencyRate(ctx context.Context, req *pricingv1.EscalateCurrencyRateRequest) (*pricingv1.EscalateCurrencyRateResponse, error) {
	if err := s.store.SaveCurrencyRate(ctx, req.GetContext().GetTenantId(), req.GetCurrencyRate()); err != nil {
		return nil, grpcx.Wrap(err, "save currency rate")
	}
	s.log.InfoContext(ctx, "escalate currency rate", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "currency_rate_id", req.GetCurrencyRate().GetCurrencyRateId())
	var envelope pricingv1.PriceListRef
	if err := req.GetCurrencyRate().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	terminateCurrencyRateRequest := &pricingv1.TerminateCurrencyRateRequest{
		Context:        req.GetContext(),
		CurrencyRateId: tenancy.Value(ctx, "currency_rate_id"),
		Details: &pricingv1.TerminateCurrencyRateRequest_Details{
			NewOwner: &identityv1.UserRef{
				DisplayName: req.GetCurrencyRate().GetSubject().GetDisplayName(),
			},
			ReasonCode: envelope.GetDisplayName(),
		},
	}
	terminateCurrencyRateReply, err := grpcx.Call(ctx, s.currencyRateLifecycleClient.TerminateCurrencyRate, terminateCurrencyRateRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateLifecycleService/TerminateCurrencyRate")
	}
	startPriceListWorkflowRequest := &pricingv1.StartPriceListWorkflowRequest{
		Context: req.GetContext(),
		PriceList: &pricingv1.PriceList{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetCurrencyRate().GetSubject().GetDisplayName(),
				UserId:      req.GetDetails().GetApprover().GetUserId(),
			},
		},
	}
	startPriceListWorkflowReply := new(pricingv1.StartPriceListWorkflowResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", pricingPriceListWorkflowServiceName, "StartPriceListWorkflow"), startPriceListWorkflowRequest, startPriceListWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListWorkflowService/StartPriceListWorkflow")
	}
	step := &currencyRateApprovalServerEscalateCurrencyRateCloneMetricSeriesStep{request: &metricsv1.CloneMetricSeriesRequest{
		Context: req.GetContext(),
		MetricSeries: &metricsv1.MetricSeries{
			Audit: &commonv1.AuditInfo{
				Etag: startPriceListWorkflowReply.GetPriceList().GetAudit().GetEtag(),
			},
		},
	}}
	if err := s.runEscalateCurrencyRateCloneMetricSeries(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesService/CloneMetricSeries")
	}
	cloneMetricSeriesReply := step.reply
	return &pricingv1.EscalateCurrencyRateResponse{
		Outcome: &pricingv1.EscalateCurrencyRateResponse_Outcome{
			Status:      terminateCurrencyRateReply.GetResult().GetStatus(),
			ReferenceId: startPriceListWorkflowReply.GetOutcome().GetReferenceId(),
		},
		CurrencyRate: &pricingv1.CurrencyRate{
			CorrelationId:  startPriceListWorkflowReply.GetResult().GetCorrelationId(),
			CurrencyRateId: req.GetCurrencyRateId(),
		},
		Result: &commonv1.OperationResult{
			Details:       cloneMetricSeriesReply.GetResult().GetDetails(),
			CorrelationId: req.GetCurrencyRate().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// RecallCurrencyRate recalls a currency rate.
func (s *CurrencyRateApprovalServer) RecallCurrencyRate(ctx context.Context, req *pricingv1.RecallCurrencyRateRequest) (*pricingv1.RecallCurrencyRateResponse, error) {
	if err := s.store.SaveCurrencyRate(ctx, req.GetContext().GetTenantId(), req.GetCurrencyRate()); err != nil {
		return nil, grpcx.Wrap(err, "save currency rate")
	}
	s.log.InfoContext(ctx, "recall currency rate", "tenant", req.GetContext().GetTenantId())
	auditEventValidation, ok := s.clients.Get("acme.audit.v1.AuditEventValidationService").(auditv1.AuditEventValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.audit.v1.AuditEventValidationService")
	}
	normalizeAuditEventRequest := &auditv1.NormalizeAuditEventRequest{
		Context: req.GetContext(),
		AuditEvent: &auditv1.AuditEvent{
			Source:     req.GetCurrencyRate().GetSource(),
			OccurredAt: req.GetCurrencyRate().GetOccurredAt(),
			Sequence:   req.GetCurrencyRate().GetSequence(),
		},
	}
	normalizeAuditEventReply, err := auditEventValidation.NormalizeAuditEvent(ctx, normalizeAuditEventRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventValidationService/NormalizeAuditEvent")
	}
	cloned := proto.Clone(req.GetDetails()).(*pricingv1.RecallCurrencyRateRequest_Details)
	cancelPriceListWorkflowRequest := &pricingv1.CancelPriceListWorkflowRequest{
		Context: req.GetContext(),
		PriceList: &pricingv1.PriceList{
			Owner: &identityv1.UserRef{
				Email:  cloned.GetApprover().GetEmail(),
				UserId: req.GetDetails().GetApprover().GetUserId(),
			},
		},
	}
	cancelPriceListWorkflowReply := new(pricingv1.CancelPriceListWorkflowResponse)
	if err := s.conn.Invoke(ctx, pricingPriceListWorkflowServiceCancelPriceListWorkflowMethod, cancelPriceListWorkflowRequest, cancelPriceListWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListWorkflowService/CancelPriceListWorkflow")
	}
	return &pricingv1.RecallCurrencyRateResponse{
		CurrencyRate: &pricingv1.CurrencyRate{
			CorrelationId:  normalizeAuditEventReply.GetResult().GetCorrelationId(),
			Attributes:     normalizeAuditEventReply.GetAuditEvent().GetAttributes(),
			CurrencyRateId: req.GetCurrencyRateId(),
		},
		Outcome: &pricingv1.RecallCurrencyRateResponse_Outcome{
			Message: cancelPriceListWorkflowReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: req.GetCurrencyRate().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// GetCurrencyRateApprovalStatus returns a currency rate.
func (s *CurrencyRateApprovalServer) GetCurrencyRateApprovalStatus(ctx context.Context, req *pricingv1.GetCurrencyRateApprovalStatusRequest) (*pricingv1.GetCurrencyRateApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetCurrencyRateApprovalStatus", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "currency_rate", req.GetCurrencyRateId(), "get_currency_rate_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get currency rate approval status", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "user_id", req.GetContext().GetActor().GetUserId())
	advanceCarrierRateWorkflowRequest := &shippingv1.AdvanceCarrierRateWorkflowRequest{
		Context: req.GetContext(),
		CarrierRate: &shippingv1.CarrierRate{
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: tenancy.Value(ctx, "user_id"),
			},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
		},
	}
	advanceCarrierRateWorkflowReply := new(shippingv1.AdvanceCarrierRateWorkflowResponse)
	if err := grpcx.Invoke(ctx, s.conn, shippingv1.CarrierRateWorkflowService_AdvanceCarrierRateWorkflow_FullMethodName, advanceCarrierRateWorkflowRequest, advanceCarrierRateWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateWorkflowService/AdvanceCarrierRateWorkflow")
	}
	return &pricingv1.GetCurrencyRateApprovalStatusResponse{
		Result: &commonv1.OperationResult{
			Status:        advanceCarrierRateWorkflowReply.GetOutcome().GetStatus(),
			CorrelationId: advanceCarrierRateWorkflowReply.GetResult().GetCorrelationId(),
		},
		CurrencyRate: &pricingv1.CurrencyRate{CurrencyRateId: req.GetCurrencyRateId()},
	}, nil
}

// CommentCurrencyRateApproval comments a currency rate.
func (s *CurrencyRateApprovalServer) CommentCurrencyRateApproval(ctx context.Context, req *pricingv1.CommentCurrencyRateApprovalRequest) (*pricingv1.CommentCurrencyRateApprovalResponse, error) {
	if err := s.store.SaveCurrencyRate(ctx, req.GetContext().GetTenantId(), req.GetCurrencyRate()); err != nil {
		return nil, grpcx.Wrap(err, "save currency rate")
	}
	s.log.InfoContext(ctx, "comment currency rate approval", "tenant", req.GetContext().GetTenantId())
	var envelope pricingv1.PriceListRef
	if err := req.GetCurrencyRate().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	step := &currencyRateApprovalServerCommentCurrencyRateApprovalListShippingQuoteWorkflowsStep{request: &checkoutv1.ListShippingQuoteWorkflowsRequest{
		Context: req.GetContext(),
		Filter:  &checkoutv1.ListShippingQuoteWorkflowsRequest_Filter{OwnerId: req.GetCurrencyRate().GetSubject().GetCustomerId()},
		Page:    &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}}
	if err := s.runCommentCurrencyRateApprovalListShippingQuoteWorkflows(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.ShippingQuoteWorkflowService/ListShippingQuoteWorkflows")
	}
	listShippingQuoteWorkflowsReply := step.reply
	var envelope2 pricingv1.PriceListRef
	if err := req.GetCurrencyRate().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	escalatePriceListRequest := &pricingv1.EscalatePriceListRequest{
		Context: req.GetContext(),
		PriceList: &pricingv1.PriceList{
			Owner: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
		},
		PriceListId: envelope2.GetDisplayName(),
	}
	escalatePriceListReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*pricingv1.EscalatePriceListResponse, error) {
		return s.priceListApprovalClient.EscalatePriceList(ctx, escalatePriceListRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListApprovalService/EscalatePriceList")
	}
	validatePriceQuoteRequest := buildValidatePriceQuoteRequestForCommentCurrencyRateApproval(req, escalatePriceListReply)
	validatePriceQuoteReply, err := s.priceQuoteValidationClient.ValidatePriceQuote(ctx, validatePriceQuoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteValidationService/ValidatePriceQuote")
	}
	for measurement := range iterx.Each(req.GetCurrencyRate().GetMeasurements(), func(candidate *pricingv1.CurrencyRateMeasurement) bool { return candidate != nil }) {
		if _, err := s.priceOverrideLifecycleClient.SchedulePriceOverrideExpiry(ctx, &pricingv1.SchedulePriceOverrideExpiryRequest{
			Context: req.GetContext(),
			Details: &pricingv1.SchedulePriceOverrideExpiryRequest_Details{
				Notify: &commonv1.ContactInfo{Email: req.GetCurrencyRate().GetSubject().GetEmail()},
			},
			PriceOverride: &pricingv1.PriceOverride{
				Audit: &commonv1.AuditInfo{
					UpdatedAt: measurement.GetObservedAt(),
					CreatedAt: escalatePriceListReply.GetPriceList().GetAudit().GetCreatedAt(),
				},
				Action: &pricingv1.PriceOverride_Discount{Discount: &pricingv1.PriceOverrideDiscountAction{
					Amount: escalatePriceListReply.GetPriceList().GetDiscount().GetAmount(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideLifecycleService/SchedulePriceOverrideExpiry")
		}
	}
	return &pricingv1.CommentCurrencyRateApprovalResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: listShippingQuoteWorkflowsReply.GetResult().GetReferenceIds(),
			Message:      listShippingQuoteWorkflowsReply.GetResult().GetMessage(),
			Details:      escalatePriceListReply.GetResult().GetDetails(),
			Status:       escalatePriceListReply.GetResult().GetStatus(),
		},
		Outcome: &pricingv1.CommentCurrencyRateApprovalResponse_Outcome{Status: validatePriceQuoteReply.GetOutcome().GetStatus()},
		CurrencyRate: &pricingv1.CurrencyRate{
			CurrencyRateId: req.GetCurrencyRateId(),
			Attributes:     req.GetCurrencyRate().GetAttributes(),
		},
	}, nil
}

// currencyRateApprovalServerEscalateCurrencyRateCloneMetricSeriesStep carries the request of one step of
// EscalateCurrencyRate and, once it ran, its reply.
type currencyRateApprovalServerEscalateCurrencyRateCloneMetricSeriesStep struct {
	request *metricsv1.CloneMetricSeriesRequest
	reply   *metricsv1.CloneMetricSeriesResponse
}

func (s *CurrencyRateApprovalServer) runEscalateCurrencyRateCloneMetricSeries(ctx context.Context, step *currencyRateApprovalServerEscalateCurrencyRateCloneMetricSeriesStep) error {
	reply, err := s.metricSeriesClient.CloneMetricSeries(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// currencyRateApprovalServerCommentCurrencyRateApprovalListShippingQuoteWorkflowsStep carries the request of one step
// of CommentCurrencyRateApproval and, once it ran, its reply.
type currencyRateApprovalServerCommentCurrencyRateApprovalListShippingQuoteWorkflowsStep struct {
	request *checkoutv1.ListShippingQuoteWorkflowsRequest
	reply   *checkoutv1.ListShippingQuoteWorkflowsResponse
}

func (s *CurrencyRateApprovalServer) runCommentCurrencyRateApprovalListShippingQuoteWorkflows(ctx context.Context, step *currencyRateApprovalServerCommentCurrencyRateApprovalListShippingQuoteWorkflowsStep) error {
	reply, err := s.shippingQuoteWorkflowClient.ListShippingQuoteWorkflows(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
