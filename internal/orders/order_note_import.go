package orders

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// OrderNoteImportServer implements `acme.orders.v1.OrderNoteImportService`.
// Takes order notes in bulk from partners and legacy systems.
type OrderNoteImportServer struct {
	ordersv1.UnimplementedOrderNoteImportServiceServer
	store                           *Store
	log                             *slog.Logger
	backorderValidationClient       ordersv1.BackorderValidationServiceClient
	orderAmendmentCalculationClient ordersv1.OrderAmendmentCalculationServiceClient
	conn                            *grpc.ClientConn
	householdQueryClient            customersv1.HouseholdQueryServiceClient
	orderReportingClient            ordersv1.OrderReportingServiceClient
	metricSeriesClient              metricsv1.MetricSeriesServiceClient
	placementWorkflowClient         merchandisingv1.PlacementWorkflowServiceClient
	consentRecordValidationClient   customersv1.ConsentRecordValidationServiceClient
	orderNoteValidationClient       ordersv1.OrderNoteValidationServiceClient
}

// NewOrderNoteImportServer wires an OrderNoteImportServer.
func NewOrderNoteImportServer(deps platform.Deps) *OrderNoteImportServer {
	return &OrderNoteImportServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.orders.v1.OrderNoteImportService"),
		backorderValidationClient:       ordersv1.NewBackorderValidationServiceClient(deps.Conn),
		orderAmendmentCalculationClient: ordersv1.NewOrderAmendmentCalculationServiceClient(deps.Conn),
		conn:                            deps.Conn,
		householdQueryClient:            customersv1.NewHouseholdQueryServiceClient(deps.Conn),
		orderReportingClient:            ordersv1.NewOrderReportingServiceClient(deps.Conn),
		metricSeriesClient:              metricsv1.NewMetricSeriesServiceClient(deps.Conn),
		placementWorkflowClient:         merchandisingv1.NewPlacementWorkflowServiceClient(deps.Conn),
		consentRecordValidationClient:   customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		orderNoteValidationClient:       ordersv1.NewOrderNoteValidationServiceClient(deps.Conn),
	}
}

// ImportOrderNotes takes a stream of order notes and answers once at the end.
func (s *OrderNoteImportServer) ImportOrderNotes(stream grpc.ClientStreamingServer[ordersv1.ImportOrderNotesRequest, ordersv1.ImportOrderNotesResponse]) error {
	ctx := stream.Context()
	var accepted int64
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		s.log.InfoContext(ctx, "import order notes", "tenant", msg.GetContext().GetTenantId())
		validateBackorderRequest := &ordersv1.ValidateBackorderRequest{
			Context: msg.GetContext(),
			Details: &ordersv1.ValidateBackorderRequest_Details{
				Contact: &commonv1.ContactInfo{Phone: msg.GetHeader().GetSubmittedBy().GetPhone()},
			},
			Backorder: &ordersv1.Backorder{
				Requester: &commonv1.ContactInfo{
					Email: msg.GetHeader().GetSubmittedBy().GetEmail(),
					Name:  msg.GetHeader().GetSubmittedBy().GetName(),
				},
			},
		}
		if _, err := grpcx.Call(ctx, s.backorderValidationClient.ValidateBackorder, validateBackorderRequest); err != nil {
			return grpcx.Wrap(err, "acme.orders.v1.BackorderValidationService/ValidateBackorder")
		}
		recalculateOrderAmendmentsRequest := &ordersv1.RecalculateOrderAmendmentsRequest{Context: msg.GetContext()}
		if _, err := grpcx.Call(ctx, s.orderAmendmentCalculationClient.RecalculateOrderAmendments, recalculateOrderAmendmentsRequest); err != nil {
			return grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentCalculationService/RecalculateOrderAmendments")
		}
		accepted++
	}
	return stream.SendAndClose(&ordersv1.ImportOrderNotesResponse{Accepted: accepted})
}

// GetOrderNoteImportStatus returns an order note.
func (s *OrderNoteImportServer) GetOrderNoteImportStatus(ctx context.Context, req *ordersv1.GetOrderNoteImportStatusRequest) (*ordersv1.GetOrderNoteImportStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_note", req.GetOrderNoteId(), "get_order_note_import_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get order note import status", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	startRewardWorkflowRequest := &loyaltyv1.StartRewardWorkflowRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.StartRewardWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		Reward: &loyaltyv1.Reward{
			Owner:      &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Audit:      &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
			Parameters: baggage,
		},
	}
	startRewardWorkflowReply, err := grpcx.NewClient(s.conn, loyaltyv1.NewRewardWorkflowServiceClient).StartRewardWorkflow(ctx, startRewardWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardWorkflowService/StartRewardWorkflow")
	}
	return &ordersv1.GetOrderNoteImportStatusResponse{
		Result: &commonv1.OperationResult{Status: startRewardWorkflowReply.GetResult().GetStatus()},
		OrderNote: &ordersv1.OrderNote{
			CorrelationId: startRewardWorkflowReply.GetResult().GetCorrelationId(),
			OrderNoteId:   req.GetOrderNoteId(),
		},
	}, nil
}

// CancelOrderNoteImport cancels an order note.
func (s *OrderNoteImportServer) CancelOrderNoteImport(ctx context.Context, req *ordersv1.CancelOrderNoteImportRequest) (*ordersv1.CancelOrderNoteImportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_note", req.GetOrderNoteId(), "cancel_order_note_import"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "cancel order note import", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*ordersv1.CancelOrderNoteImportRequest_Details)
	suggestHouseholdsRequest := &customersv1.SuggestHouseholdsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.SuggestHouseholdsRequest_Filter{
			Labels:       req.GetAnnotations(),
			OwnerId:      cloned.GetNewOwner().GetUserId(),
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
		},
	}
	type suggestHouseholdsResult struct {
		reply *customersv1.SuggestHouseholdsResponse
		err   error
	}
	suggestHouseholdsResults := make(chan suggestHouseholdsResult, 1)
	go func() {
		reply, err := s.householdQueryClient.SuggestHouseholds(ctx, suggestHouseholdsRequest)
		suggestHouseholdsResults <- suggestHouseholdsResult{reply, err}
	}()
	var suggestHouseholdsReply *customersv1.SuggestHouseholdsResponse
	select {
	case result := <-suggestHouseholdsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.HouseholdQueryService/SuggestHouseholds")
		}
		suggestHouseholdsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	shareSellerScorecardReportRequest := &sellersv1.ShareSellerScorecardReportRequest{
		Context: req.GetContext(),
		Details: &sellersv1.ShareSellerScorecardReportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email:            req.GetDetails().GetNewOwner().GetEmail(),
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
				Name:             req.GetDetails().GetNotifyContact().GetName(),
			},
			Payload: packed,
		},
	}
	shareSellerScorecardReportReply := new(sellersv1.ShareSellerScorecardReportResponse)
	if err := s.conn.Invoke(ctx, sellersSellerScorecardReportingServiceShareSellerScorecardReportMethod, shareSellerScorecardReportRequest, shareSellerScorecardReportReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardReportingService/ShareSellerScorecardReport")
	}
	resolveOrderAmendmentsRequest := &ordersv1.ResolveOrderAmendmentsRequest{Context: req.GetContext()}
	resolveOrderAmendmentsReply, err := resolveOrderAmendmentsCall(ctx, resolveOrderAmendmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentQueryService/ResolveOrderAmendments")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeOrderAmendmentsRequest := &ordersv1.SummarizeOrderAmendmentsRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      req.GetReason(),
		Details: &ordersv1.SummarizeOrderAmendmentsRequest_Details{
			Filters: shareSellerScorecardReportReply.GetSellerScorecard().GetMetadata(),
		},
	}
	summarizeOrderAmendmentsReply := new(ordersv1.SummarizeOrderAmendmentsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", ordersOrderAmendmentQueryServiceName, "SummarizeOrderAmendments"), summarizeOrderAmendmentsRequest, summarizeOrderAmendmentsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentQueryService/SummarizeOrderAmendments")
	}
	return &ordersv1.CancelOrderNoteImportResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  suggestHouseholdsReply.GetResult().GetReferenceIds(),
			Details:       shareSellerScorecardReportReply.GetResult().GetDetails(),
			CorrelationId: resolveOrderAmendmentsReply.GetResult().GetCorrelationId(),
			Message:       summarizeOrderAmendmentsReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		Outcome: &ordersv1.CancelOrderNoteImportResponse_Outcome{
			Message:     suggestHouseholdsReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		OrderNote: &ordersv1.OrderNote{OrderNoteId: req.GetOrderNoteId()},
	}, nil
}

// ValidateOrderNoteImport validates an order note.
func (s *OrderNoteImportServer) ValidateOrderNoteImport(ctx context.Context, req *ordersv1.ValidateOrderNoteImportRequest) (*ordersv1.ValidateOrderNoteImportResponse, error) {
	if err := s.store.SaveOrderNote(ctx, req.GetContext().GetTenantId(), req.GetOrderNote()); err != nil {
		return nil, grpcx.Wrap(err, "save order note")
	}
	s.log.InfoContext(ctx, "validate order note import", "tenant", req.GetContext().GetTenantId())
	var envelope ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	retryPaymentWorkflowRequest := &paymentsv1.RetryPaymentWorkflowRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.RetryPaymentWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Name:  req.GetDetails().GetContact().GetName(),
				Email: req.GetDetails().GetContact().GetEmail(),
			},
		},
		Payment: &paymentsv1.Payment{
			Counterparty: &paymentsv1.RefundRef{Number: req.GetOrderNote().GetSubject().GetNumber()},
			Lines:        []*paymentsv1.PaymentLine{&paymentsv1.PaymentLine{Attributes: req.GetOrderNote().GetAttributes()}},
		},
		PaymentId: envelope.GetDisplayName(),
	}
	retryPaymentWorkflowReply := new(paymentsv1.RetryPaymentWorkflowResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", paymentsPaymentWorkflowServiceName, "RetryPaymentWorkflow"), retryPaymentWorkflowRequest, retryPaymentWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentWorkflowService/RetryPaymentWorkflow")
	}
	var envelope2 ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	streamOrderMetricsStream, err := s.orderReportingClient.StreamOrderMetrics(ctx, &ordersv1.StreamOrderMetricsRequest{
		Context: req.GetContext(),
		Filter: &ordersv1.StreamOrderMetricsRequest_Filter{
			Region: retryPaymentWorkflowReply.GetPayment().GetBillingAddress().GetRegion(),
			Query:  envelope2.GetDisplayName(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderReportingService/StreamOrderMetrics")
	}
	for {
		received, err := streamOrderMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.orders.v1.OrderReportingService/StreamOrderMetrics")
		}
		_ = received
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*metricsv1.CloneMetricSeriesResponse, len(req.GetOrderNote().GetMeasurements()))
	for i, measurement := range req.GetOrderNote().GetMeasurements() {
		group.Go(func() error {
			reply, err := s.metricSeriesClient.CloneMetricSeries(groupCtx, &metricsv1.CloneMetricSeriesRequest{
				Context: req.GetContext(),
				MetricSeries: &metricsv1.MetricSeries{
					Dimensions: &metricsv1.MetricSeriesDimensions{Unit: measurement.GetUnit()},
					Audit: &commonv1.AuditInfo{
						CreatedBy: retryPaymentWorkflowReply.GetPayment().GetAudit().GetCreatedBy(),
						CreatedAt: measurement.GetObservedAt(),
					},
					Owner: &identityv1.UserRef{Email: req.GetDetails().GetContact().GetEmail()},
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
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesService/CloneMetricSeries")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &ordersv1.ValidateOrderNoteImportResponse{
		Outcome: &ordersv1.ValidateOrderNoteImportResponse_Outcome{
			ReferenceId: retryPaymentWorkflowReply.GetOutcome().GetReferenceId(),
		},
		OrderNote: &ordersv1.OrderNote{
			OrderNoteId: req.GetOrderNoteId(),
			Sequence:    req.GetOrderNote().GetSequence(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListOrderNoteImports lists order notes a page at a time.
func (s *OrderNoteImportServer) ListOrderNoteImports(ctx context.Context, req *ordersv1.ListOrderNoteImportsRequest) (*ordersv1.ListOrderNoteImportsResponse, error) {
	s.log.InfoContext(ctx, "list order note imports", "tenant", req.GetContext().GetTenantId())
	return &ordersv1.ListOrderNoteImportsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// RetryOrderNoteImport retrys an order note.
func (s *OrderNoteImportServer) RetryOrderNoteImport(ctx context.Context, req *ordersv1.RetryOrderNoteImportRequest) (*ordersv1.RetryOrderNoteImportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RetryOrderNoteImport", "error", err)
		return nil, err
	}
	if err := s.store.SaveOrderNote(ctx, req.GetContext().GetTenantId(), req.GetOrderNote()); err != nil {
		return nil, grpcx.Wrap(err, "save order note")
	}
	s.log.InfoContext(ctx, "retry order note import", "tenant", req.GetContext().GetTenantId())
	var envelope ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	step := &orderNoteImportServerRetryOrderNoteImportPausePlacementWorkflowStep{request: &merchandisingv1.PausePlacementWorkflowRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Participants: []*merchandisingv1.PlacementParticipant{&merchandisingv1.PlacementParticipant{
				Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotify().GetPhone()},
			}},
		},
		Details: &merchandisingv1.PausePlacementWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: req.GetDetails().GetNotify().GetEmail(),
				Name:  req.GetDetails().GetNotify().GetName(),
			},
		},
		PlacementId: envelope.GetDisplayName(),
	}}
	if err := s.runRetryOrderNoteImportPausePlacementWorkflow(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementWorkflowService/PausePlacementWorkflow")
	}
	pausePlacementWorkflowReply := step.reply
	return &ordersv1.RetryOrderNoteImportResponse{
		Outcome: &ordersv1.RetryOrderNoteImportResponse_Outcome{
			Status: pausePlacementWorkflowReply.GetOutcome().GetStatus(),
		},
		OrderNote: &ordersv1.OrderNote{
			OrderNoteId: req.GetOrderNoteId(),
			Sequence:    req.GetOrderNote().GetSequence(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// MapOrderNoteImportColumns maps an order note.
func (s *OrderNoteImportServer) MapOrderNoteImportColumns(ctx context.Context, req *ordersv1.MapOrderNoteImportColumnsRequest) (*ordersv1.MapOrderNoteImportColumnsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected MapOrderNoteImportColumns", "error", err)
		return nil, err
	}
	if err := s.store.SaveOrderNote(ctx, req.GetContext().GetTenantId(), req.GetOrderNote()); err != nil {
		return nil, grpcx.Wrap(err, "save order note")
	}
	s.log.InfoContext(ctx, "map order note import columns", "tenant", req.GetContext().GetTenantId())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*customersv1.PreviewConsentRecordResponse, len(req.GetOrderNote().GetMeasurements()))
	for i, measurement := range req.GetOrderNote().GetMeasurements() {
		group.Go(func() error {
			reply, err := s.consentRecordValidationClient.PreviewConsentRecord(groupCtx, &customersv1.PreviewConsentRecordRequest{
				Context: req.GetContext(),
				ConsentRecord: &customersv1.ConsentRecord{
					Measurements: []*customersv1.ConsentRecordMeasurement{&customersv1.ConsentRecordMeasurement{
						Metric:     measurement.GetMetric(),
						ObservedAt: measurement.GetObservedAt(),
						Unit:       measurement.GetUnit(),
					}},
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
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/PreviewConsentRecord")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	packed, err := anypb.New(req.GetOrderNote())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	lintOrderNotesReply, err := s.orderNoteValidationClient.LintOrderNotes(ctx, &ordersv1.LintOrderNotesRequest{
		Context: req.GetContext(),
		Details: &ordersv1.LintOrderNotesRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetContact().GetPhone(),
				Name:  req.GetDetails().GetContact().GetName(),
			},
		},
		OrderNote: &ordersv1.OrderNote{Detail: packed},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteValidationService/LintOrderNotes")
	}
	for _, measurement2 := range req.GetOrderNote().GetMeasurements() {
		if _, err := s.orderAmendmentCalculationClient.SimulateOrderAmendment(ctx, &ordersv1.SimulateOrderAmendmentRequest{
			Context: req.GetContext(),
			OrderAmendment: &ordersv1.OrderAmendment{
				Audit: &commonv1.AuditInfo{UpdatedAt: measurement2.GetObservedAt()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentCalculationService/SimulateOrderAmendment")
		}
	}
	return &ordersv1.MapOrderNoteImportColumnsResponse{
		OrderNote: &ordersv1.OrderNote{
			OccurredAt:  lintOrderNotesReply.GetOrderNote().GetOccurredAt(),
			OrderNoteId: req.GetOrderNoteId(),
			Source:      req.GetOrderNote().GetSource(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// orderNoteImportServerRetryOrderNoteImportPausePlacementWorkflowStep carries the request of one step of
// RetryOrderNoteImport and, once it ran, its reply.
type orderNoteImportServerRetryOrderNoteImportPausePlacementWorkflowStep struct {
	request *merchandisingv1.PausePlacementWorkflowRequest
	reply   *merchandisingv1.PausePlacementWorkflowResponse
}

func (s *OrderNoteImportServer) runRetryOrderNoteImportPausePlacementWorkflow(ctx context.Context, step *orderNoteImportServerRetryOrderNoteImportPausePlacementWorkflowStep) error {
	reply, err := s.placementWorkflowClient.PausePlacementWorkflow(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
