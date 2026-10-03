package returns

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	employeesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/employees/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	treasuryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/treasury/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// ReturnLabelValidationServer implements `acme.returns.v1.ReturnLabelValidationService`.
// Checks return labels against the rules before anything acts on them.
type ReturnLabelValidationServer struct {
	returnsv1.UnimplementedReturnLabelValidationServiceServer
	store                            *Store
	log                              *slog.Logger
	searchQueryNotificationClient    searchv1.SearchQueryNotificationServiceClient
	employeeClient                   employeesv1.EmployeeServiceClient
	returnLabelCalculationClient     returnsv1.ReturnLabelCalculationServiceClient
	conn                             *grpc.ClientConn
	returnRequestValidationClient    returnsv1.ReturnRequestValidationServiceClient
	bus                              *eventbus.Bus
	estimateInspectionFn             func(context.Context, *returnsv1.EstimateInspectionRequest, ...grpc.CallOption) (*returnsv1.EstimateInspectionResponse, error)
	tenantNotificationClient         identityv1.TenantNotificationServiceClient
	subscriptionPlanValidationClient subscriptionsv1.SubscriptionPlanValidationServiceClient
	quoteGiftCardTransactionFn       func(context.Context, *giftcardsv1.QuoteGiftCardTransactionRequest, ...grpc.CallOption) (*giftcardsv1.QuoteGiftCardTransactionResponse, error)
	sellerScorecardCalculationClient sellersv1.SellerScorecardCalculationServiceClient
	cashPositionCalculationPort      cashPositionCalculationPort
}

// NewReturnLabelValidationServer wires a ReturnLabelValidationServer.
func NewReturnLabelValidationServer(deps platform.Deps) *ReturnLabelValidationServer {
	inspectionCalculation := returnsv1.NewInspectionCalculationServiceClient(deps.Conn)
	giftCardTransactionCalculation := giftcardsv1.NewGiftCardTransactionCalculationServiceClient(deps.Conn)
	return &ReturnLabelValidationServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.returns.v1.ReturnLabelValidationService"),
		searchQueryNotificationClient:    searchv1.NewSearchQueryNotificationServiceClient(deps.Conn),
		employeeClient:                   employeesv1.NewEmployeeServiceClient(deps.Conn),
		returnLabelCalculationClient:     returnsv1.NewReturnLabelCalculationServiceClient(deps.Conn),
		conn:                             deps.Conn,
		returnRequestValidationClient:    returnsv1.NewReturnRequestValidationServiceClient(deps.Conn),
		bus:                              deps.Bus,
		estimateInspectionFn:             inspectionCalculation.EstimateInspection,
		tenantNotificationClient:         identityv1.NewTenantNotificationServiceClient(deps.Conn),
		subscriptionPlanValidationClient: subscriptionsv1.NewSubscriptionPlanValidationServiceClient(deps.Conn),
		quoteGiftCardTransactionFn:       giftCardTransactionCalculation.QuoteGiftCardTransaction,
		sellerScorecardCalculationClient: sellersv1.NewSellerScorecardCalculationServiceClient(deps.Conn),
		cashPositionCalculationPort:      newCashPositionCalculationPort(deps),
	}
}

// ValidateReturnLabel validates a return label.
func (s *ReturnLabelValidationServer) ValidateReturnLabel(ctx context.Context, req *returnsv1.ValidateReturnLabelRequest) (*returnsv1.ValidateReturnLabelResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReturnLabel(ctx, req.GetContext().GetTenantId(), req.GetReturnLabel()); err != nil {
		return nil, grpcx.Wrap(err, "save return label")
	}
	s.log.InfoContext(ctx, "validate return label", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnLabel().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "region", req.GetReturnLabel().GetBillingAddress().GetRegion())
	streamSearchQueryNoticesStream, err := s.searchQueryNotificationClient.StreamSearchQueryNotices(ctx, &searchv1.StreamSearchQueryNoticesRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			Email:            req.GetDetails().GetContact().GetEmail(),
			Phone:            req.GetDetails().GetContact().GetPhone(),
		},
		Filter: &searchv1.StreamSearchQueryNoticesRequest_Filter{Region: tenancy.Value(ctx, "region")},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/StreamSearchQueryNotices")
	}
	for {
		received, err := streamSearchQueryNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/StreamSearchQueryNotices")
		}
		_ = received
	}
	batchGetEmployeesReply, err := s.employeeClient.BatchGetEmployees(ctx, &employeesv1.BatchGetEmployeesRequest{Context: req.GetContext()})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.employees.v1.EmployeeService/BatchGetEmployees")
	}
	getReturnLabelRatesReply, err := s.returnLabelCalculationClient.GetReturnLabelRates(ctx, &returnsv1.GetReturnLabelRatesRequest{
		Context:       req.GetContext(),
		ReturnLabelId: req.GetReturnLabel().GetReturnLabelId(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/GetReturnLabelRates")
	}
	calculateSellerScorecardRequest := &sellersv1.CalculateSellerScorecardRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			Settlement: &sellersv1.SellerScorecard_Card{Card: &sellersv1.SellerScorecardCardSettlement{
				Last4: getReturnLabelRatesReply.GetReturnLabel().GetCard().GetLast4(),
			}},
		},
	}
	calculateSellerScorecardReply := new(sellersv1.CalculateSellerScorecardResponse)
	if err := s.conn.Invoke(ctx, sellersSellerScorecardCalculationServiceCalculateSellerScorecardMethod, calculateSellerScorecardRequest, calculateSellerScorecardReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardCalculationService/CalculateSellerScorecard")
	}
	return &returnsv1.ValidateReturnLabelResponse{
		Outcome: &returnsv1.ValidateReturnLabelResponse_Outcome{
			Status:  batchGetEmployeesReply.GetResult().GetStatus(),
			Message: getReturnLabelRatesReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{Status: batchGetEmployeesReply.GetResult().GetStatus()},
		ReturnLabel: &returnsv1.ReturnLabel{
			Notes: getReturnLabelRatesReply.GetReturnLabel().GetNotes(),
			BillingAddress: &commonv1.PostalAddress{
				Recipient: calculateSellerScorecardReply.GetSellerScorecard().GetBillingAddress().GetRecipient(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: calculateSellerScorecardReply.GetSellerScorecard().GetAudit().GetCreatedAt(),
			},
			ReturnLabelId: req.GetReturnLabelId(),
			Total:         req.GetReturnLabel().GetTotal(),
		},
	}, nil
}

// PreviewReturnLabel previews a return label.
func (s *ReturnLabelValidationServer) PreviewReturnLabel(ctx context.Context, req *returnsv1.PreviewReturnLabelRequest) (*returnsv1.PreviewReturnLabelResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReturnLabel(ctx, req.GetContext().GetTenantId(), req.GetReturnLabel()); err != nil {
		return nil, grpcx.Wrap(err, "save return label")
	}
	s.log.InfoContext(ctx, "preview return label", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnLabel().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetReturnLabel()).(*returnsv1.ReturnLabel)
	searchSearchIndexesRequest := &searchv1.SearchSearchIndexesRequest{
		Context: req.GetContext(),
		Filter:  &searchv1.SearchSearchIndexesRequest_Filter{Region: cloned.GetBillingAddress().GetRegion()},
	}
	searchSearchIndexesReply := new(searchv1.SearchSearchIndexesResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", searchSearchIndexServiceName, "SearchSearchIndexes"), searchSearchIndexesRequest, searchSearchIndexesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexService/SearchSearchIndexes")
	}
	explainReturnRequestDecisionRequest := buildExplainReturnRequestDecisionRequestForPreviewReturnLabel(req)
	explainReturnRequestDecisionReply, err := s.returnRequestValidationClient.ExplainReturnRequestDecision(ctx, explainReturnRequestDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestValidationService/ExplainReturnRequestDecision")
	}
	return &returnsv1.PreviewReturnLabelResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: searchSearchIndexesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		ReturnLabel: &returnsv1.ReturnLabel{
			Notes:         explainReturnRequestDecisionReply.GetReturnRequest().GetNotes(),
			ReturnLabelId: req.GetReturnLabelId(),
			DueAt:         req.GetReturnLabel().GetDueAt(),
		},
	}, nil
}

// NormalizeReturnLabel normalizes a return label.
func (s *ReturnLabelValidationServer) NormalizeReturnLabel(ctx context.Context, req *returnsv1.NormalizeReturnLabelRequest) (*returnsv1.NormalizeReturnLabelResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected NormalizeReturnLabel", "error", err)
		return nil, err
	}
	if err := s.store.SaveReturnLabel(ctx, req.GetContext().GetTenantId(), req.GetReturnLabel()); err != nil {
		return nil, grpcx.Wrap(err, "save return label")
	}
	s.log.InfoContext(ctx, "normalize return label", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnLabel().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "returns.normalize_return_label.quote_return_label",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"return_label.audit.updated_by": req.GetReturnLabel().GetAudit().GetUpdatedBy(),
			"context.tenant_id":             req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	simulateReturnLabelRequest := buildSimulateReturnLabelRequestForNormalizeReturnLabel(req)
	simulateReturnLabelReply, err := s.returnLabelCalculationClient.SimulateReturnLabel(ctx, simulateReturnLabelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/SimulateReturnLabel")
	}
	estimateInspectionRequest := &returnsv1.EstimateInspectionRequest{Context: req.GetContext()}
	estimateInspectionReply, err := s.estimateInspectionFn(ctx, estimateInspectionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionCalculationService/EstimateInspection")
	}
	return &returnsv1.NormalizeReturnLabelResponse{
		ReturnLabel: &returnsv1.ReturnLabel{
			Number: simulateReturnLabelReply.GetReturnLabel().GetNumber(),
			Counterparty: &returnsv1.InspectionRef{
				DisplayName: estimateInspectionReply.GetInspection().GetAssignee().GetDisplayName(),
			},
			ReturnLabelId:  req.GetReturnLabelId(),
			BillingAddress: req.GetReturnLabel().GetBillingAddress(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainReturnLabelDecision explains a return label.
func (s *ReturnLabelValidationServer) ExplainReturnLabelDecision(ctx context.Context, req *returnsv1.ExplainReturnLabelDecisionRequest) (*returnsv1.ExplainReturnLabelDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReturnLabel(ctx, req.GetContext().GetTenantId(), req.GetReturnLabel()); err != nil {
		return nil, grpcx.Wrap(err, "save return label")
	}
	s.log.InfoContext(ctx, "explain return label decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnLabel().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "return_label_id", req.GetReturnLabel().GetReturnLabelId())
	getReturnLabelRatesRequest := &returnsv1.GetReturnLabelRatesRequest{
		Context:       req.GetContext(),
		ReturnLabelId: tenancy.Value(ctx, "return_label_id"),
	}
	send := s.getReturnLabelRatesSender(getReturnLabelRatesRequest)
	getReturnLabelRatesReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/GetReturnLabelRates")
	}
	return &returnsv1.ExplainReturnLabelDecisionResponse{
		ReturnLabel: &returnsv1.ReturnLabel{
			BillingAddress: &commonv1.PostalAddress{
				Locality: getReturnLabelRatesReply.GetReturnLabel().GetBillingAddress().GetLocality(),
			},
			IssuedAt:      getReturnLabelRatesReply.GetReturnLabel().GetIssuedAt(),
			ReturnLabelId: req.GetReturnLabelId(),
			Total:         req.GetReturnLabel().GetTotal(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScoreReturnLabel scores a return label.
func (s *ReturnLabelValidationServer) ScoreReturnLabel(ctx context.Context, req *returnsv1.ScoreReturnLabelRequest) (*returnsv1.ScoreReturnLabelResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReturnLabel(ctx, req.GetContext().GetTenantId(), req.GetReturnLabel()); err != nil {
		return nil, grpcx.Wrap(err, "save return label")
	}
	s.log.InfoContext(ctx, "score return label", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnLabel().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	sendTenantTestNoticeReply, err := s.tenantNotificationClient.SendTenantTestNotice(ctx, &identityv1.SendTenantTestNoticeRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      req.GetReason(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantNotificationService/SendTenantTestNotice")
	}
	quoteReturnLabelRequest := &returnsv1.QuoteReturnLabelRequest{
		Context: req.GetContext(),
		ReturnLabel: &returnsv1.ReturnLabel{
			BillingAddress: &commonv1.PostalAddress{
				Locality: sendTenantTestNoticeReply.GetTenant().GetLocation().GetLocality(),
			},
		},
	}
	quoteReturnLabelReply, err := grpcx.NewClient(s.conn, returnsv1.NewReturnLabelCalculationServiceClient).QuoteReturnLabel(ctx, quoteReturnLabelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/QuoteReturnLabel")
	}
	return &returnsv1.ScoreReturnLabelResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: sendTenantTestNoticeReply.GetResult().GetReferenceIds(),
			Status:       sendTenantTestNoticeReply.GetResult().GetStatus(),
			Details:      quoteReturnLabelReply.GetResult().GetDetails(),
		},
		Breakdown: &returnsv1.ScoreReturnLabelResponse_Breakdown{
			CurrencyCode: quoteReturnLabelReply.GetBreakdown().GetCurrencyCode(),
		},
		ReturnLabel: &returnsv1.ReturnLabel{
			ReturnLabelId: req.GetReturnLabelId(),
			Status:        req.GetReturnLabel().GetStatus(),
		},
	}, nil
}

// LintReturnLabels lints a return label.
func (s *ReturnLabelValidationServer) LintReturnLabels(ctx context.Context, req *returnsv1.LintReturnLabelsRequest) (*returnsv1.LintReturnLabelsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReturnLabel(ctx, req.GetContext().GetTenantId(), req.GetReturnLabel()); err != nil {
		return nil, grpcx.Wrap(err, "save return label")
	}
	s.log.InfoContext(ctx, "lint return labels", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReturnLabel().GetAudit().GetEtag())
	getReturnLabelRatesRequest := buildGetReturnLabelRatesRequestForLintReturnLabels(req)
	getReturnLabelRatesReply, err := s.returnLabelCalculationClient.GetReturnLabelRates(ctx, getReturnLabelRatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/GetReturnLabelRates")
	}
	previewSubscriptionPlanRequest := &subscriptionsv1.PreviewSubscriptionPlanRequest{
		Context: req.GetContext(),
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetReturnLabel().GetAudit().GetCreatedAt(),
				UpdatedBy: req.GetReturnLabel().GetAudit().GetUpdatedBy(),
			},
		},
		Details: &subscriptionsv1.PreviewSubscriptionPlanRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
		},
	}
	previewSubscriptionPlanReply, err := subscriptionsv1.SubscriptionPlanValidationServiceClient.PreviewSubscriptionPlan(s.subscriptionPlanValidationClient, ctx, previewSubscriptionPlanRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanValidationService/PreviewSubscriptionPlan")
	}
	packed, err := anypb.New(req.GetReturnLabel())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	quoteGiftCardTransactionRequest := &giftcardsv1.QuoteGiftCardTransactionRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.QuoteGiftCardTransactionRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: getReturnLabelRatesReply.GetReturnLabel().GetBillingAddress().GetCountryCode(),
			},
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{Detail: packed},
	}
	quoteGiftCardTransactionReply, err := s.quoteGiftCardTransactionFn(ctx, quoteGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/QuoteGiftCardTransaction")
	}
	for _, line := range req.GetReturnLabel().GetLines() {
		if _, err := s.sellerScorecardCalculationClient.RecalculateSellerScorecards(ctx, &sellersv1.RecalculateSellerScorecardsRequest{
			Context: req.GetContext(),
			SellerScorecard: &sellersv1.SellerScorecard{
				Lines: []*sellersv1.SellerScorecardLine{&sellersv1.SellerScorecardLine{
					Amount: line.GetTax().GetAmount(),
					Tax: &sellersv1.SellerScorecardLineTax{
						Jurisdiction: line.GetTax().GetJurisdiction(),
						Rate:         line.GetTax().GetRate(),
					},
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardCalculationService/RecalculateSellerScorecards")
		}
	}
	return &returnsv1.LintReturnLabelsResponse{
		ReturnLabel: &returnsv1.ReturnLabel{
			Audit: &commonv1.AuditInfo{
				CreatedBy: getReturnLabelRatesReply.GetReturnLabel().GetAudit().GetCreatedBy(),
			},
			Total:         getReturnLabelRatesReply.GetReturnLabel().GetTotal(),
			Number:        quoteGiftCardTransactionReply.GetGiftCardTransaction().GetSubject().GetNumber(),
			ReturnLabelId: req.GetReturnLabelId(),
			Notes:         req.GetReturnLabel().GetNotes(),
		},
		Result: &commonv1.OperationResult{
			Status:       previewSubscriptionPlanReply.GetResult().GetStatus(),
			ReferenceIds: quoteGiftCardTransactionReply.GetResult().GetReferenceIds(),
		},
	}, nil
}

// GetReturnLabelRules returns a return label.
func (s *ReturnLabelValidationServer) GetReturnLabelRules(ctx context.Context, req *returnsv1.GetReturnLabelRulesRequest) (*returnsv1.GetReturnLabelRulesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "return_label", req.GetReturnLabelId(), "get_return_label_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get return label rules", "tenant", req.GetContext().GetTenantId())
	quoteCashPositionRequest := &treasuryv1.QuoteCashPositionRequest{
		Context: req.GetContext(),
		CashPosition: &treasuryv1.CashPosition{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	quoteCashPositionReply, err := s.cashPositionCalculationPort.QuoteCashPosition(ctx, quoteCashPositionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.treasury.v1.CashPositionCalculationService/QuoteCashPosition")
	}
	return &returnsv1.GetReturnLabelRulesResponse{
		ReturnLabel: &returnsv1.ReturnLabel{
			Total:         quoteCashPositionReply.GetBreakdown().GetTotal(),
			ReturnLabelId: req.GetReturnLabelId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// getReturnLabelRatesSender returns a function that sends one request to
// acme.returns.v1.ReturnLabelCalculationService/GetReturnLabelRates.
func (s *ReturnLabelValidationServer) getReturnLabelRatesSender(request *returnsv1.GetReturnLabelRatesRequest) func(context.Context) (*returnsv1.GetReturnLabelRatesResponse, error) {
	return func(ctx context.Context) (*returnsv1.GetReturnLabelRatesResponse, error) {
		return s.returnLabelCalculationClient.GetReturnLabelRates(ctx, request)
	}
}
