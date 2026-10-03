package returns

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ReturnLabelWorkflowServer implements `acme.returns.v1.ReturnLabelWorkflowService`.
// Runs the long-lived workflow that takes a return label from start to finish.
type ReturnLabelWorkflowServer struct {
	returnsv1.UnimplementedReturnLabelWorkflowServiceServer
	store                        *Store
	log                          *slog.Logger
	meteredHouseholdQuery        meteredHouseholdQuery
	metricSeriesQueryClient      metricsv1.MetricSeriesQueryServiceClient
	conn                         *grpc.ClientConn
	bus                          *eventbus.Bus
	returnLabelCalculationClient returnsv1.ReturnLabelCalculationServiceClient
	usageRecordApprovalClient    subscriptionsv1.UsageRecordApprovalServiceClient
	searchQueryQueryClient       searchv1.SearchQueryQueryServiceClient
	clients                      *registry.Registry
	sellerScorecardSyncClient    sellersv1.SellerScorecardSyncServiceClient
	returnRequestApprovalClient  returnsv1.ReturnRequestApprovalServiceClient
}

// NewReturnLabelWorkflowServer wires a ReturnLabelWorkflowServer.
func NewReturnLabelWorkflowServer(deps platform.Deps) *ReturnLabelWorkflowServer {
	return &ReturnLabelWorkflowServer{
		store:                        NewStore(deps.DB),
		log:                          deps.Logger.With("service", "acme.returns.v1.ReturnLabelWorkflowService"),
		meteredHouseholdQuery:        meteredHouseholdQuery{HouseholdQueryServiceClient: customersv1.NewHouseholdQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		metricSeriesQueryClient:      metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn),
		conn:                         deps.Conn,
		bus:                          deps.Bus,
		returnLabelCalculationClient: returnsv1.NewReturnLabelCalculationServiceClient(deps.Conn),
		usageRecordApprovalClient:    subscriptionsv1.NewUsageRecordApprovalServiceClient(deps.Conn),
		searchQueryQueryClient:       searchv1.NewSearchQueryQueryServiceClient(deps.Conn),
		clients:                      deps.Clients,
		sellerScorecardSyncClient:    sellersv1.NewSellerScorecardSyncServiceClient(deps.Conn),
		returnRequestApprovalClient:  returnsv1.NewReturnRequestApprovalServiceClient(deps.Conn),
	}
}

// StartReturnLabelWorkflow starts a return label.
func (s *ReturnLabelWorkflowServer) StartReturnLabelWorkflow(ctx context.Context, req *returnsv1.StartReturnLabelWorkflowRequest) (*returnsv1.StartReturnLabelWorkflowResponse, error) {
	if err := s.store.SaveReturnLabel(ctx, req.GetContext().GetTenantId(), req.GetReturnLabel()); err != nil {
		return nil, grpcx.Wrap(err, "save return label")
	}
	s.log.InfoContext(ctx, "start return label workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnLabel().GetAudit().GetEtag())
	countHouseholdsRequest := &customersv1.CountHouseholdsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.CountHouseholdsRequest_Filter{
			Region: req.GetReturnLabel().GetBillingAddress().GetRegion(),
		},
	}
	countHouseholdsReply, err := s.meteredHouseholdQuery.CountHouseholds(ctx, countHouseholdsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/CountHouseholds")
	}
	return &returnsv1.StartReturnLabelWorkflowResponse{
		Result: &commonv1.OperationResult{Status: countHouseholdsReply.GetResult().GetStatus()},
		ReturnLabel: &returnsv1.ReturnLabel{
			ReturnLabelId:  req.GetReturnLabelId(),
			BillingAddress: req.GetReturnLabel().GetBillingAddress(),
		},
	}, nil
}

// AdvanceReturnLabelWorkflow advances a return label.
func (s *ReturnLabelWorkflowServer) AdvanceReturnLabelWorkflow(ctx context.Context, req *returnsv1.AdvanceReturnLabelWorkflowRequest) (*returnsv1.AdvanceReturnLabelWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReturnLabel(ctx, req.GetContext().GetTenantId(), req.GetReturnLabel()); err != nil {
		return nil, grpcx.Wrap(err, "save return label")
	}
	s.log.InfoContext(ctx, "advance return label workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnLabel().GetAudit().GetEtag())
	summarizeMetricSeriesesRequest := buildSummarizeMetricSeriesesRequestForAdvanceReturnLabelWorkflow(req)
	summarizeMetricSeriesesReply, err := s.metricSeriesQueryClient.SummarizeMetricSerieses(ctx, summarizeMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/SummarizeMetricSerieses")
	}
	returnLabelCache := cache.New[string, *returnsv1.ReturnLabel](1)
	returnLabelCache.Put(req.GetContext().GetTraceId(), req.GetReturnLabel())
	cachedReturnLabel, _ := returnLabelCache.Get(req.GetContext().GetTraceId())
	var last4 string
	switch choice := req.GetReturnLabel().GetSettlement().(type) {
	case *returnsv1.ReturnLabel_Card:
		last4 = choice.Card.GetLast4()
	case *returnsv1.ReturnLabel_Bank:
		last4 = choice.Bank.GetIban()
	case *returnsv1.ReturnLabel_VoucherCode:
		last4 = choice.VoucherCode
	}
	approveReturnLabelRequest := &returnsv1.ApproveReturnLabelRequest{
		Context: req.GetContext(),
		ReturnLabel: &returnsv1.ReturnLabel{
			BillingAddress: &commonv1.PostalAddress{
				Locality: cachedReturnLabel.GetBillingAddress().GetLocality(),
			},
			Settlement: &returnsv1.ReturnLabel_Card{Card: &returnsv1.ReturnLabelCardSettlement{
				Last4:   last4,
				Network: req.GetReturnLabel().GetCard().GetNetwork(),
			}},
		},
	}
	approveReturnLabelReply := new(returnsv1.ApproveReturnLabelResponse)
	if err := grpcx.Invoke(ctx, s.conn, returnsv1.ReturnLabelApprovalService_ApproveReturnLabel_FullMethodName, approveReturnLabelRequest, approveReturnLabelReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelApprovalService/ApproveReturnLabel")
	}
	return &returnsv1.AdvanceReturnLabelWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status: summarizeMetricSeriesesReply.GetResult().GetStatus(),
		},
		Outcome: &returnsv1.AdvanceReturnLabelWorkflowResponse_Outcome{
			Message: summarizeMetricSeriesesReply.GetResult().GetMessage(),
		},
		ReturnLabel: &returnsv1.ReturnLabel{
			Counterparty: &returnsv1.InspectionRef{
				DisplayName: approveReturnLabelReply.GetReturnLabel().GetCounterparty().GetDisplayName(),
			},
			ReturnLabelId: req.GetReturnLabelId(),
			Notes:         req.GetReturnLabel().GetNotes(),
		},
	}, nil
}

// CancelReturnLabelWorkflow cancels a return label.
func (s *ReturnLabelWorkflowServer) CancelReturnLabelWorkflow(ctx context.Context, req *returnsv1.CancelReturnLabelWorkflowRequest) (*returnsv1.CancelReturnLabelWorkflowResponse, error) {
	if err := s.store.SaveReturnLabel(ctx, req.GetContext().GetTenantId(), req.GetReturnLabel()); err != nil {
		return nil, grpcx.Wrap(err, "save return label")
	}
	s.log.InfoContext(ctx, "cancel return label workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnLabel().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "returns.cancel_return_label_workflow.resolve_seller_scorecard_conflict",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"seller_scorecard.audit.created_by": req.GetReturnLabel().GetAudit().GetCreatedBy(),
			"context.tenant_id":                 req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	estimateReturnLabelReply, err := s.returnLabelCalculationClient.EstimateReturnLabel(ctx, &returnsv1.EstimateReturnLabelRequest{
		Context: req.GetContext(),
		ReturnLabel: &returnsv1.ReturnLabel{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetReturnLabel().GetAudit().GetCreatedBy()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/EstimateReturnLabel")
	}
	return &returnsv1.CancelReturnLabelWorkflowResponse{
		ReturnLabel: &returnsv1.ReturnLabel{
			Counterparty: &returnsv1.InspectionRef{
				Priority: estimateReturnLabelReply.GetReturnLabel().GetCounterparty().GetPriority(),
			},
			BillingAddress: estimateReturnLabelReply.GetReturnLabel().GetBillingAddress(),
			ReturnLabelId:  req.GetReturnLabelId(),
			Status:         req.GetReturnLabel().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PauseReturnLabelWorkflow pauses a return label.
func (s *ReturnLabelWorkflowServer) PauseReturnLabelWorkflow(ctx context.Context, req *returnsv1.PauseReturnLabelWorkflowRequest) (*returnsv1.PauseReturnLabelWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PauseReturnLabelWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveReturnLabel(ctx, req.GetContext().GetTenantId(), req.GetReturnLabel()); err != nil {
		return nil, grpcx.Wrap(err, "save return label")
	}
	s.log.InfoContext(ctx, "pause return label workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnLabel().GetAudit().GetEtag())
	for _, line := range req.GetReturnLabel().GetLines() {
		if _, err := s.usageRecordApprovalClient.RecallUsageRecord(ctx, &subscriptionsv1.RecallUsageRecordRequest{
			Context: req.GetContext(),
			UsageRecord: &subscriptionsv1.UsageRecord{
				Attributes: line.GetAttributes(),
				Subject:    &customersv1.CustomerRef{Email: req.GetDetails().GetNotify().GetEmail()},
			},
			Details: &subscriptionsv1.RecallUsageRecordRequest_Details{
				Approver: &identityv1.UserRef{
					DisplayName: req.GetReturnLabel().GetCounterparty().GetDisplayName(),
				},
			},
			Annotations: req.GetAnnotations(),
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordApprovalService/RecallUsageRecord")
		}
	}
	exportSearchQueriesStream, err := s.searchQueryQueryClient.ExportSearchQueries(ctx, &searchv1.ExportSearchQueriesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ExportSearchQueriesRequest_Filter{
			Region:       req.GetReturnLabel().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetReturnLabel().GetAudit().GetCreatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/ExportSearchQueries")
	}
	for {
		received, err := exportSearchQueriesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/ExportSearchQueries")
		}
		_ = received
	}
	return &returnsv1.PauseReturnLabelWorkflowResponse{
		ReturnLabel: &returnsv1.ReturnLabel{
			ReturnLabelId: req.GetReturnLabelId(),
			Metadata:      req.GetReturnLabel().GetMetadata(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// InspectReturnLabelWorkflow returns a return label.
func (s *ReturnLabelWorkflowServer) InspectReturnLabelWorkflow(ctx context.Context, req *returnsv1.InspectReturnLabelWorkflowRequest) (*returnsv1.InspectReturnLabelWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected InspectReturnLabelWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "return_label", req.GetReturnLabelId(), "inspect_return_label_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect return label workflow", "tenant", req.GetContext().GetTenantId())
	searchQueryQuery, ok := s.clients.Get("acme.search.v1.SearchQueryQueryService").(searchv1.SearchQueryQueryServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.search.v1.SearchQueryQueryService")
	}
	summarizeSearchQueriesRequest := &searchv1.SummarizeSearchQueriesRequest{
		Context: req.GetContext(),
		Details: &searchv1.SummarizeSearchQueriesRequest_Details{Filters: req.GetContext().GetBaggage()},
	}
	summarizeSearchQueriesReply, err := searchQueryQuery.SummarizeSearchQueries(ctx, summarizeSearchQueriesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/SummarizeSearchQueries")
	}
	streamSellerScorecardChangesStream, err := s.sellerScorecardSyncClient.StreamSellerScorecardChanges(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardSyncService/StreamSellerScorecardChanges")
	}
	if err := streamSellerScorecardChangesStream.Send(&sellersv1.StreamSellerScorecardChangesRequest{
		Context: req.GetContext(),
		Frame:   &sellersv1.StreamSellerScorecardChangesRequest_Ack{Ack: &sellersv1.StreamSellerScorecardChangesRequest_ChangeAck{Status: summarizeSearchQueriesReply.GetResult().GetStatus()}},
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardSyncService/StreamSellerScorecardChanges")
	}
	if err := streamSellerScorecardChangesStream.CloseSend(); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardSyncService/StreamSellerScorecardChanges")
	}
	acknowledged, err := streamSellerScorecardChangesStream.Recv()
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardSyncService/StreamSellerScorecardChanges")
	}
	_ = acknowledged
	return &returnsv1.InspectReturnLabelWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message:       summarizeSearchQueriesReply.GetResult().GetMessage(),
			CorrelationId: summarizeSearchQueriesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		ReturnLabel: &returnsv1.ReturnLabel{ReturnLabelId: req.GetReturnLabelId()},
	}, nil
}

// ListReturnLabelWorkflows lists return labels a page at a time.
func (s *ReturnLabelWorkflowServer) ListReturnLabelWorkflows(ctx context.Context, req *returnsv1.ListReturnLabelWorkflowsRequest) (*returnsv1.ListReturnLabelWorkflowsResponse, error) {
	s.log.InfoContext(ctx, "list return label workflows", "tenant", req.GetContext().GetTenantId())
	return &returnsv1.ListReturnLabelWorkflowsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SignalReturnLabelWorkflow signals a return label.
func (s *ReturnLabelWorkflowServer) SignalReturnLabelWorkflow(ctx context.Context, req *returnsv1.SignalReturnLabelWorkflowRequest) (*returnsv1.SignalReturnLabelWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReturnLabel(ctx, req.GetContext().GetTenantId(), req.GetReturnLabel()); err != nil {
		return nil, grpcx.Wrap(err, "save return label")
	}
	s.log.InfoContext(ctx, "signal return label workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnLabel().GetAudit().GetEtag())
	exportSearchQueriesStream, err := s.searchQueryQueryClient.ExportSearchQueries(ctx, &searchv1.ExportSearchQueriesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ExportSearchQueriesRequest_Filter{
			Region:       req.GetReturnLabel().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetReturnLabel().GetAudit().GetCreatedAt(),
			Labels:       req.GetReturnLabel().GetMetadata(),
			OwnerId:      req.GetReturnLabel().GetReturnLabelId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/ExportSearchQueries")
	}
	for {
		received, err := exportSearchQueriesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/ExportSearchQueries")
		}
		_ = received
	}
	returnLabelCache := cache.New[string, *returnsv1.ReturnLabel](1)
	returnLabelCache.Put(req.GetContext().GetTraceId(), req.GetReturnLabel())
	cachedReturnLabel, _ := returnLabelCache.Get(req.GetContext().GetTraceId())
	submitReturnRequestRequest := &returnsv1.SubmitReturnRequestRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: cachedReturnLabel.GetBillingAddress().GetPostalCode(),
			},
		},
	}
	type submitReturnRequestResult struct {
		reply *returnsv1.SubmitReturnRequestResponse
		err   error
	}
	submitReturnRequestResults := make(chan submitReturnRequestResult, 1)
	go func() {
		reply, err := s.returnRequestApprovalClient.SubmitReturnRequest(ctx, submitReturnRequestRequest)
		submitReturnRequestResults <- submitReturnRequestResult{reply, err}
	}()
	var submitReturnRequestReply *returnsv1.SubmitReturnRequestResponse
	select {
	case result := <-submitReturnRequestResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.returns.v1.ReturnRequestApprovalService/SubmitReturnRequest")
		}
		submitReturnRequestReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &returnsv1.SignalReturnLabelWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: submitReturnRequestReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		ReturnLabel: &returnsv1.ReturnLabel{
			ReturnLabelId: req.GetReturnLabelId(),
			Number:        req.GetReturnLabel().GetNumber(),
		},
	}, nil
}
