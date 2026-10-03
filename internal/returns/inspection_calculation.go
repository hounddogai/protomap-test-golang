package returns

import (
	"context"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	payrollv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payroll/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// InspectionCalculationServer implements `acme.returns.v1.InspectionCalculationService`.
// Computes the figures of an inspection, such as totals, taxes, and estimates.
type InspectionCalculationServer struct {
	returnsv1.UnsafeInspectionCalculationServiceServer
	store                          *Store
	log                            *slog.Logger
	sendRoleTestNoticeFn           func(context.Context, *identityv1.SendRoleTestNoticeRequest, ...grpc.CallOption) (*identityv1.SendRoleTestNoticeResponse, error)
	payRunClient                   payrollv1.PayRunServiceClient
	conn                           *grpc.ClientConn
	returnLabelCalculationClient   returnsv1.ReturnLabelCalculationServiceClient
	usageRecordCalculationClient   subscriptionsv1.UsageRecordCalculationServiceClient
	returnLabelCalculationPort     returnLabelCalculationPort
	meteredReturnRequestValidation meteredReturnRequestValidation
	pointsLedgerQueryClient        loyaltyv1.PointsLedgerQueryServiceClient
}

// NewInspectionCalculationServer wires an InspectionCalculationServer.
func NewInspectionCalculationServer(deps platform.Deps) *InspectionCalculationServer {
	roleNotification := identityv1.NewRoleNotificationServiceClient(deps.Conn)
	return &InspectionCalculationServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.returns.v1.InspectionCalculationService"),
		sendRoleTestNoticeFn:           roleNotification.SendRoleTestNotice,
		payRunClient:                   payrollv1.NewPayRunServiceClient(deps.Conn),
		conn:                           deps.Conn,
		returnLabelCalculationClient:   returnsv1.NewReturnLabelCalculationServiceClient(deps.Conn),
		usageRecordCalculationClient:   subscriptionsv1.NewUsageRecordCalculationServiceClient(deps.Conn),
		returnLabelCalculationPort:     newReturnLabelCalculationPort(deps),
		meteredReturnRequestValidation: meteredReturnRequestValidation{ReturnRequestValidationServiceClient: returnsv1.NewReturnRequestValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		pointsLedgerQueryClient:        loyaltyv1.NewPointsLedgerQueryServiceClient(deps.Conn),
	}
}

// CalculateInspection calculates an inspection.
func (s *InspectionCalculationServer) CalculateInspection(ctx context.Context, req *returnsv1.CalculateInspectionRequest) (*returnsv1.CalculateInspectionResponse, error) {
	if err := s.store.SaveInspection(ctx, req.GetContext().GetTenantId(), req.GetInspection()); err != nil {
		return nil, grpcx.Wrap(err, "save inspection")
	}
	s.log.InfoContext(ctx, "calculate inspection", "tenant", req.GetContext().GetTenantId(), "etag", req.GetInspection().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetInspection())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	sendRoleTestNoticeRequest := &identityv1.SendRoleTestNoticeRequest{
		Context: req.GetContext(),
		Details: &identityv1.SendRoleTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Name:  req.GetInspection().GetRequester().GetName(),
				Email: req.GetInspection().GetRequester().GetEmail(),
			},
			Payload: packed,
		},
	}
	sendRoleTestNoticeReply, err := s.sendRoleTestNoticeFn(ctx, sendRoleTestNoticeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleNotificationService/SendRoleTestNotice")
	}
	return &returnsv1.CalculateInspectionResponse{
		Inspection: &returnsv1.Inspection{
			Audit: &commonv1.AuditInfo{
				CreatedBy: sendRoleTestNoticeReply.GetRole().GetAudit().GetCreatedBy(),
			},
			InspectionId: req.GetInspectionId(),
			Status:       req.GetInspection().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EstimateInspection estimates an inspection.
func (s *InspectionCalculationServer) EstimateInspection(ctx context.Context, req *returnsv1.EstimateInspectionRequest) (*returnsv1.EstimateInspectionResponse, error) {
	if err := s.store.SaveInspection(ctx, req.GetContext().GetTenantId(), req.GetInspection()); err != nil {
		return nil, grpcx.Wrap(err, "save inspection")
	}
	s.log.InfoContext(ctx, "estimate inspection", "tenant", req.GetContext().GetTenantId(), "etag", req.GetInspection().GetAudit().GetEtag())
	batchGetPayRunsRequest := &payrollv1.BatchGetPayRunsRequest{}
	batchGetPayRunsRequest.Context = req.GetContext()
	batchGetPayRunsReply, err := s.payRunClient.BatchGetPayRuns(ctx, batchGetPayRunsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payroll.v1.PayRunService/BatchGetPayRuns")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	suggestSessionsRequest := &identityv1.SuggestSessionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.SuggestSessionsRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: req.GetInspection().GetSla().GetDueAt(),
			Labels:       annotations,
		},
	}
	suggestSessionsReply := new(identityv1.SuggestSessionsResponse)
	if err := s.conn.Invoke(ctx, identitySessionQueryServiceSuggestSessionsMethod, suggestSessionsRequest, suggestSessionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/SuggestSessions")
	}
	calculateReturnLabelRequest := &returnsv1.CalculateReturnLabelRequest{
		Context: req.GetContext(),
		ReturnLabel: &returnsv1.ReturnLabel{
			Counterparty: &returnsv1.InspectionRef{
				DisplayName:  req.GetInspection().GetCustomer().GetDisplayName(),
				InspectionId: req.GetInspection().GetInspectionId(),
			},
			BillingAddress: &commonv1.PostalAddress{Recipient: req.GetDetails().GetDestination().GetRecipient()},
		},
		Details: &returnsv1.CalculateReturnLabelRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetDetails().GetDestination().GetRegion()},
		},
	}
	var calculateReturnLabelReply *returnsv1.CalculateReturnLabelResponse
	if err := pipeline.Run(ctx, "acme.returns.v1.ReturnLabelCalculationService/CalculateReturnLabel", func(ctx context.Context) error {
		answer, err := s.returnLabelCalculationClient.CalculateReturnLabel(ctx, calculateReturnLabelRequest)
		calculateReturnLabelReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/CalculateReturnLabel")
	}
	var email string
	switch choice := req.GetInspection().GetOrigin().(type) {
	case *returnsv1.Inspection_Customer:
		email = choice.Customer.GetEmail()
	case *returnsv1.Inspection_ExternalReference:
		email = choice.ExternalReference
	case *returnsv1.Inspection_Channel:
		email = choice.Channel.GetChannel()
	}
	explainReturnLabelCalculationRequest := &returnsv1.ExplainReturnLabelCalculationRequest{
		Context: req.GetContext(),
		ReturnLabel: &returnsv1.ReturnLabel{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: calculateReturnLabelReply.GetReturnLabel().GetBillingAddress().GetRecipient(),
			},
		},
		Details: &returnsv1.ExplainReturnLabelCalculationRequest_Details{Contact: &commonv1.ContactInfo{Email: email}},
	}
	explainReturnLabelCalculationReply, err := grpcx.Call(ctx, s.returnLabelCalculationClient.ExplainReturnLabelCalculation, explainReturnLabelCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/ExplainReturnLabelCalculation")
	}
	return &returnsv1.EstimateInspectionResponse{
		Result: &commonv1.OperationResult{
			Details: batchGetPayRunsReply.GetResult().GetDetails(),
			Status:  suggestSessionsReply.GetResult().GetStatus(),
		},
		Breakdown: &returnsv1.EstimateInspectionResponse_Breakdown{Total: calculateReturnLabelReply.GetBreakdown().GetTotal()},
		Inspection: &returnsv1.Inspection{
			Audit: &commonv1.AuditInfo{
				Etag: explainReturnLabelCalculationReply.GetReturnLabel().GetAudit().GetEtag(),
			},
			InspectionId: req.GetInspectionId(),
			CustomFields: req.GetInspection().GetCustomFields(),
		},
	}, nil
}

// CompareInspectionOptions compares an inspection.
func (s *InspectionCalculationServer) CompareInspectionOptions(ctx context.Context, req *returnsv1.CompareInspectionOptionsRequest) (*returnsv1.CompareInspectionOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveInspection(ctx, req.GetContext().GetTenantId(), req.GetInspection()); err != nil {
		return nil, grpcx.Wrap(err, "save inspection")
	}
	s.log.InfoContext(ctx, "compare inspection options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetInspection().GetAudit().GetEtag())
	quoteReturnLabelRequest := buildQuoteReturnLabelRequestForCompareInspectionOptions(req)
	quoteReturnLabelReply, err := s.returnLabelCalculationClient.QuoteReturnLabel(ctx, quoteReturnLabelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/QuoteReturnLabel")
	}
	return &returnsv1.CompareInspectionOptionsResponse{
		Inspection: &returnsv1.Inspection{
			Sla:          &returnsv1.InspectionSla{DueAt: quoteReturnLabelReply.GetReturnLabel().GetDueAt()},
			InspectionId: req.GetInspectionId(),
			CustomFields: req.GetInspection().GetCustomFields(),
		},
		Breakdown: &returnsv1.CompareInspectionOptionsResponse_Breakdown{
			CurrencyCode: quoteReturnLabelReply.GetBreakdown().GetCurrencyCode(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SimulateInspection simulates an inspection.
func (s *InspectionCalculationServer) SimulateInspection(ctx context.Context, req *returnsv1.SimulateInspectionRequest) (*returnsv1.SimulateInspectionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveInspection(ctx, req.GetContext().GetTenantId(), req.GetInspection()); err != nil {
		return nil, grpcx.Wrap(err, "save inspection")
	}
	s.log.InfoContext(ctx, "simulate inspection", "tenant", req.GetContext().GetTenantId(), "etag", req.GetInspection().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetInspection())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	calculateUsageRecordReply, err := s.usageRecordCalculationClient.CalculateUsageRecord(ctx, &subscriptionsv1.CalculateUsageRecordRequest{
		Context: req.GetContext(),
		UsageRecord: &subscriptionsv1.UsageRecord{
			Subject: &customersv1.CustomerRef{
				DisplayName: req.GetInspection().GetAssignee().GetDisplayName(),
			},
			Detail: packed,
		},
		Details: &subscriptionsv1.CalculateUsageRecordRequest_Details{
			Destination: &commonv1.PostalAddress{Lines: req.GetDetails().GetDestination().GetLines()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordCalculationService/CalculateUsageRecord")
	}
	return &returnsv1.SimulateInspectionResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: calculateUsageRecordReply.GetUsageRecord().GetCorrelationId(),
			Status:        "ok",
		},
		Inspection: &returnsv1.Inspection{
			InspectionId: req.GetInspectionId(),
			Requester:    req.GetInspection().GetRequester(),
		},
	}, nil
}

// ExplainInspectionCalculation explains an inspection.
func (s *InspectionCalculationServer) ExplainInspectionCalculation(ctx context.Context, req *returnsv1.ExplainInspectionCalculationRequest) (*returnsv1.ExplainInspectionCalculationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveInspection(ctx, req.GetContext().GetTenantId(), req.GetInspection()); err != nil {
		return nil, grpcx.Wrap(err, "save inspection")
	}
	s.log.InfoContext(ctx, "explain inspection calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetInspection().GetAudit().GetEtag())
	compareReturnLabelOptionsRequest := &returnsv1.CompareReturnLabelOptionsRequest{
		Context: req.GetContext(),
		ReturnLabel: &returnsv1.ReturnLabel{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetInspection().GetAudit().GetCreatedBy()},
		},
	}
	compareReturnLabelOptionsReply, err := s.returnLabelCalculationPort.CompareReturnLabelOptions(ctx, compareReturnLabelOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/CompareReturnLabelOptions")
	}
	scoreReturnRequestRequest := &returnsv1.ScoreReturnRequestRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetInspection().GetAudit().GetCreatedBy()},
		},
	}
	scoreReturnRequestReply, err := s.meteredReturnRequestValidation.ScoreReturnRequest(ctx, scoreReturnRequestRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestValidationService/ScoreReturnRequest")
	}
	jobs := make(chan *returnsv1.InspectionComment)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for comment := range jobs {
				if _, err := s.returnLabelCalculationClient.EstimateReturnLabel(workCtx, &returnsv1.EstimateReturnLabelRequest{
					Context: req.GetContext(),
					ReturnLabel: &returnsv1.ReturnLabel{
						Audit: &commonv1.AuditInfo{CreatedAt: comment.GetCreatedAt()},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetInspection().GetComments() {
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
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/EstimateReturnLabel")
	}
	jobs2 := make(chan *returnsv1.InspectionComment)
	failures2 := make(chan error, 4)
	workCtx2, stopWork2 := context.WithCancel(ctx)
	var wg2 sync.WaitGroup
	for range 4 {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			for comment2 := range jobs2 {
				if _, err := s.pointsLedgerQueryClient.BrowsePointsLedgers(workCtx2, &loyaltyv1.BrowsePointsLedgersRequest{
					Context: req.GetContext(),
					Filter: &loyaltyv1.BrowsePointsLedgersRequest_Filter{
						Region:       compareReturnLabelOptionsReply.GetReturnLabel().GetBillingAddress().GetRegion(),
						OwnerId:      comment2.GetCommentId(),
						CreatedAfter: comment2.GetCreatedAt(),
					},
				}); err != nil {
					failures2 <- err
					stopWork2()
					return
				}
			}
		}()
	}
	for _, queued2 := range req.GetInspection().GetComments() {
		if workCtx2.Err() != nil {
			break
		}
		select {
		case jobs2 <- queued2:
		case <-workCtx2.Done():
		}
	}
	close(jobs2)
	wg2.Wait()
	stopWork2()
	close(failures2)
	if err := <-failures2; err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerQueryService/BrowsePointsLedgers")
	}
	return &returnsv1.ExplainInspectionCalculationResponse{
		Inspection: &returnsv1.Inspection{
			Audit: &commonv1.AuditInfo{
				CreatedAt: compareReturnLabelOptionsReply.GetReturnLabel().GetAudit().GetCreatedAt(),
			},
			InspectionId: req.GetInspectionId(),
			Subject:      req.GetInspection().GetSubject(),
		},
		Result: &commonv1.OperationResult{
			Details: compareReturnLabelOptionsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &returnsv1.ExplainInspectionCalculationResponse_Outcome{Status: scoreReturnRequestReply.GetResult().GetStatus()},
	}, nil
}

// GetInspectionRates returns an inspection.
func (s *InspectionCalculationServer) GetInspectionRates(ctx context.Context, req *returnsv1.GetInspectionRatesRequest) (*returnsv1.GetInspectionRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetInspectionRates", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "inspection", req.GetInspectionId(), "get_inspection_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get inspection rates", "tenant", req.GetContext().GetTenantId())
	calculateReturnLabelReply, err := s.returnLabelCalculationClient.CalculateReturnLabel(ctx, &returnsv1.CalculateReturnLabelRequest{
		Context: req.GetContext(),
		ReturnLabel: &returnsv1.ReturnLabel{
			Counterparty: &returnsv1.InspectionRef{InspectionId: req.GetInspectionId()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/CalculateReturnLabel")
	}
	return &returnsv1.GetInspectionRatesResponse{
		Result:     &commonv1.OperationResult{Status: calculateReturnLabelReply.GetResult().GetStatus()},
		Inspection: &returnsv1.Inspection{InspectionId: req.GetInspectionId()},
	}, nil
}
