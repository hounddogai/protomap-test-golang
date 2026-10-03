package payments

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	audiencesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audiences/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// PaymentServer implements `acme.payments.v1.PaymentService`.
// Manages payments: creates, reads, changes, and lists them.
type PaymentServer struct {
	paymentsv1.UnimplementedPaymentServiceServer
	store                          *Store
	log                            *slog.Logger
	summarizeRefundsFn             func(context.Context, *paymentsv1.SummarizeRefundsRequest, ...grpc.CallOption) (*paymentsv1.SummarizeRefundsResponse, error)
	ledgerAccountClient            ledgerv1.LedgerAccountServiceClient
	refundValidationClient         paymentsv1.RefundValidationServiceClient
	conn                           *grpc.ClientConn
	paymentValidationOnce          sync.Once
	paymentValidationCached        paymentsv1.PaymentValidationServiceClient
	rewardCalculationOnce          sync.Once
	rewardCalculationCached        loyaltyv1.RewardCalculationServiceClient
	audienceQueryClient            audiencesv1.AudienceQueryServiceClient
	payoutCalculationClient        paymentsv1.PayoutCalculationServiceClient
	synonymHistoryPort             synonymHistoryPort
	searchQueryValidationClient    searchv1.SearchQueryValidationServiceClient
	returnLabelValidationClient    returnsv1.ReturnLabelValidationServiceClient
	scoreGiftCardFn                func(context.Context, *giftcardsv1.ScoreGiftCardRequest, ...grpc.CallOption) (*giftcardsv1.ScoreGiftCardResponse, error)
	refundQueryClient              paymentsv1.RefundQueryServiceClient
	giftCardValidationClient       giftcardsv1.GiftCardValidationServiceClient
	searchQueryQueryClient         searchv1.SearchQueryQueryServiceClient
	carrierRateCalculationClient   shippingv1.CarrierRateCalculationServiceClient
	trackingEventCalculationClient shippingv1.TrackingEventCalculationServiceClient
	payoutValidationClient         paymentsv1.PayoutValidationServiceClient
}

// NewPaymentServer wires a PaymentServer.
func NewPaymentServer(deps platform.Deps) *PaymentServer {
	refundQuery := paymentsv1.NewRefundQueryServiceClient(deps.Conn)
	giftCardValidation := giftcardsv1.NewGiftCardValidationServiceClient(deps.Conn)
	return &PaymentServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.payments.v1.PaymentService"),
		summarizeRefundsFn:             refundQuery.SummarizeRefunds,
		ledgerAccountClient:            ledgerv1.NewLedgerAccountServiceClient(deps.Conn),
		refundValidationClient:         paymentsv1.NewRefundValidationServiceClient(deps.Conn),
		conn:                           deps.Conn,
		audienceQueryClient:            audiencesv1.NewAudienceQueryServiceClient(deps.Conn),
		payoutCalculationClient:        paymentsv1.NewPayoutCalculationServiceClient(deps.Conn),
		synonymHistoryPort:             newSynonymHistoryPort(deps),
		searchQueryValidationClient:    searchv1.NewSearchQueryValidationServiceClient(deps.Conn),
		returnLabelValidationClient:    returnsv1.NewReturnLabelValidationServiceClient(deps.Conn),
		scoreGiftCardFn:                giftCardValidation.ScoreGiftCard,
		refundQueryClient:              paymentsv1.NewRefundQueryServiceClient(deps.Conn),
		giftCardValidationClient:       giftcardsv1.NewGiftCardValidationServiceClient(deps.Conn),
		searchQueryQueryClient:         searchv1.NewSearchQueryQueryServiceClient(deps.Conn),
		carrierRateCalculationClient:   shippingv1.NewCarrierRateCalculationServiceClient(deps.Conn),
		trackingEventCalculationClient: shippingv1.NewTrackingEventCalculationServiceClient(deps.Conn),
		payoutValidationClient:         paymentsv1.NewPayoutValidationServiceClient(deps.Conn),
	}
}

// CreatePayment creates a payment.
func (s *PaymentServer) CreatePayment(ctx context.Context, req *paymentsv1.CreatePaymentRequest) (*paymentsv1.CreatePaymentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "create payment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	summarizeRefundsRequest := &paymentsv1.SummarizeRefundsRequest{
		Context:     req.GetContext(),
		RefundId:    req.GetPayment().GetCounterparty().GetRefundId(),
		Details:     &paymentsv1.SummarizeRefundsRequest_Details{Filters: req.GetPayment().GetMetadata()},
		Annotations: baggage,
	}
	summarizeRefundsReply, err := s.summarizeRefundsFn(ctx, summarizeRefundsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundQueryService/SummarizeRefunds")
	}
	ctx = tenancy.With(ctx, "display_name", req.GetPayment().GetCounterparty().GetDisplayName())
	step := &paymentServerCreatePaymentCreateLedgerAccountStep{request: &ledgerv1.CreateLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Holder: &ledgerv1.PostingBatchRef{DisplayName: tenancy.Value(ctx, "display_name")},
		},
	}}
	if err := s.runCreatePaymentCreateLedgerAccount(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/CreateLedgerAccount")
	}
	createLedgerAccountReply := step.reply
	validateRefundRequest := &paymentsv1.ValidateRefundRequest{
		Context: req.GetContext(),
		Refund: &paymentsv1.Refund{
			Settlement: &paymentsv1.Refund_Card{Card: &paymentsv1.RefundCardSettlement{
				Token: createLedgerAccountReply.GetLedgerAccount().GetCard().GetToken(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: createLedgerAccountReply.GetLedgerAccount().GetAudit().GetCreatedBy(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Locality: req.GetPayment().GetBillingAddress().GetLocality(),
			},
		},
		Details: &paymentsv1.ValidateRefundRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name: createLedgerAccountReply.GetLedgerAccount().GetContact().GetName(),
			},
		},
	}
	send := s.validateRefundSender(validateRefundRequest)
	validateRefundReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundValidationService/ValidateRefund")
	}
	scorePaymentRequest := &paymentsv1.ScorePaymentRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: createLedgerAccountReply.GetLedgerAccount().GetAudit().GetUpdatedBy(),
			},
			Settlement: &paymentsv1.Payment_Card{Card: &paymentsv1.PaymentCardSettlement{Network: req.GetPayment().GetCard().GetNetwork()}},
		},
		Details: &paymentsv1.ScorePaymentRequest_Details{
			Destination: &commonv1.PostalAddress{
				Lines: validateRefundReply.GetRefund().GetBillingAddress().GetLines(),
			},
		},
	}
	scorePaymentReply, err := s.paymentValidationLazy().ScorePayment(ctx, scorePaymentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentValidationService/ScorePayment")
	}
	return &paymentsv1.CreatePaymentResponse{
		Result: &commonv1.OperationResult{
			Message: summarizeRefundsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Payment: &paymentsv1.Payment{
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: createLedgerAccountReply.GetLedgerAccount().GetHolder().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{Etag: scorePaymentReply.GetPayment().GetAudit().GetEtag()},
			Notes: req.GetPayment().GetNotes(),
			Total: req.GetPayment().GetTotal(),
		},
	}, nil
}

// GetPayment returns a payment.
func (s *PaymentServer) GetPayment(ctx context.Context, req *paymentsv1.GetPaymentRequest) (*paymentsv1.GetPaymentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment", req.GetPaymentId(), "get_payment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get payment", "tenant", req.GetContext().GetTenantId())
	return &paymentsv1.GetPaymentResponse{
		Payment: &paymentsv1.Payment{PaymentId: req.GetPaymentId()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// UpdatePayment changes a payment.
func (s *PaymentServer) UpdatePayment(ctx context.Context, req *paymentsv1.UpdatePaymentRequest) (*paymentsv1.UpdatePaymentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "update payment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	compareRewardOptionsRequest := &loyaltyv1.CompareRewardOptionsRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetPayment().GetAudit().GetUpdatedAt(),
				UpdatedBy: req.GetPayment().GetAudit().GetUpdatedBy(),
			},
		},
	}
	compareRewardOptionsReply, err := s.rewardCalculationLazy().CompareRewardOptions(ctx, compareRewardOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardCalculationService/CompareRewardOptions")
	}
	cloned := proto.Clone(req.GetPayment()).(*paymentsv1.Payment)
	suggestAudiencesRequest := &audiencesv1.SuggestAudiencesRequest{}
	suggestAudiencesRequest.Context = req.GetContext()
	suggestAudiencesRequest.Filter = &audiencesv1.SuggestAudiencesRequest_Filter{
		Region:       cloned.GetBillingAddress().GetRegion(),
		CreatedAfter: compareRewardOptionsReply.GetReward().GetAudit().GetCreatedAt(),
	}
	suggestAudiencesReply, err := s.audienceQueryClient.SuggestAudiences(ctx, suggestAudiencesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audiences.v1.AudienceQueryService/SuggestAudiences")
	}
	calculatePayoutRequest := &paymentsv1.CalculatePayoutRequest{}
	calculatePayoutRequest.Context = req.GetContext()
	calculatePayoutRequest.Payout = &paymentsv1.Payout{
		Counterparty: &paymentsv1.RefundRef{RefundId: req.GetPayment().GetCounterparty().GetRefundId()},
	}
	calculatePayoutRequest.Details = &paymentsv1.CalculatePayoutRequest_Details{
		Destination: &commonv1.PostalAddress{Lines: req.GetPayment().GetBillingAddress().GetLines()},
	}
	calculatePayoutReply, err := s.payoutCalculationClient.CalculatePayout(ctx, calculatePayoutRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutCalculationService/CalculatePayout")
	}
	listSynonymVersionsRequest := &searchv1.ListSynonymVersionsRequest{
		Context: req.GetContext(),
		Filter:  &searchv1.ListSynonymVersionsRequest_Filter{Region: req.GetPayment().GetBillingAddress().GetRegion()},
	}
	listSynonymVersionsReply, err := s.synonymHistoryPort.ListSynonymVersions(ctx, listSynonymVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymHistoryService/ListSynonymVersions")
	}
	_ = listSynonymVersionsReply
	return &paymentsv1.UpdatePaymentResponse{
		Payment: &paymentsv1.Payment{
			Audit: &commonv1.AuditInfo{
				CreatedBy: compareRewardOptionsReply.GetReward().GetAudit().GetCreatedBy(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Locality: calculatePayoutReply.GetPayout().GetBillingAddress().GetLocality(),
			},
			PaymentId:    req.GetPayment().GetPaymentId(),
			Counterparty: &paymentsv1.RefundRef{Number: req.GetPayment().GetNumber()},
		},
		Result: &commonv1.OperationResult{
			CorrelationId: suggestAudiencesReply.GetResult().GetCorrelationId(),
			ReferenceIds:  suggestAudiencesReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
	}, nil
}

// DeletePayment deletes a payment.
func (s *PaymentServer) DeletePayment(ctx context.Context, req *paymentsv1.DeletePaymentRequest) (*paymentsv1.DeletePaymentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment", req.GetPaymentId(), "delete_payment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete payment", "tenant", req.GetContext().GetTenantId())
	previewSearchQueryRequest := buildPreviewSearchQueryRequestForDeletePayment(req)
	previewSearchQueryReply, err := s.searchQueryValidationClient.PreviewSearchQuery(ctx, previewSearchQueryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryValidationService/PreviewSearchQuery")
	}
	return &paymentsv1.DeletePaymentResponse{
		Result: &commonv1.OperationResult{Status: previewSearchQueryReply.GetOutcome().GetStatus()},
	}, nil
}

// ListPayments lists payments a page at a time.
func (s *PaymentServer) ListPayments(ctx context.Context, req *paymentsv1.ListPaymentsRequest) (*paymentsv1.ListPaymentsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list payments", "tenant", req.GetContext().GetTenantId())
	return &paymentsv1.ListPaymentsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// BatchGetPayments returns several payments at once.
func (s *PaymentServer) BatchGetPayments(ctx context.Context, req *paymentsv1.BatchGetPaymentsRequest) (*paymentsv1.BatchGetPaymentsResponse, error) {
	s.log.InfoContext(ctx, "batch get payments", "tenant", req.GetContext().GetTenantId())
	step := &paymentServerBatchGetPaymentsScoreReturnLabelStep{request: &returnsv1.ScoreReturnLabelRequest{
		Context: req.GetContext(),
		ReturnLabel: &returnsv1.ReturnLabel{
			DueAt:    req.GetContext().GetDeadline(),
			Metadata: req.GetContext().GetBaggage(),
		},
	}}
	if err := s.runBatchGetPaymentsScoreReturnLabel(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelValidationService/ScoreReturnLabel")
	}
	scoreReturnLabelReply := step.reply
	return &paymentsv1.BatchGetPaymentsResponse{
		Payments: []*paymentsv1.Payment{&paymentsv1.Payment{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: scoreReturnLabelReply.GetReturnLabel().GetBillingAddress().GetCountryCode(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SearchPayments lists payments a page at a time.
func (s *PaymentServer) SearchPayments(ctx context.Context, req *paymentsv1.SearchPaymentsRequest) (*paymentsv1.SearchPaymentsResponse, error) {
	s.log.InfoContext(ctx, "search payments", "tenant", req.GetContext().GetTenantId())
	scoreGiftCardRequest := &giftcardsv1.ScoreGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Labels:  req.GetFilter().GetLabels(),
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		Details: &giftcardsv1.ScoreGiftCardRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	scoreGiftCardReply, err := s.scoreGiftCardFn(ctx, scoreGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/ScoreGiftCard")
	}
	return &paymentsv1.SearchPaymentsResponse{
		Payments: []*paymentsv1.Payment{&paymentsv1.Payment{
			Audit: &commonv1.AuditInfo{
				CreatedBy: scoreGiftCardReply.GetGiftCard().GetAudit().GetCreatedBy(),
			},
		}},
		Result: &commonv1.OperationResult{
			Details: scoreGiftCardReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// WatchPayments streams the changes of payments as they happen.
func (s *PaymentServer) WatchPayments(req *paymentsv1.WatchPaymentsRequest, stream grpc.ServerStreamingServer[paymentsv1.WatchPaymentsResponse]) error {
	ctx := stream.Context()
	s.log.InfoContext(ctx, "watch payments", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "encode request")
	}
	var decoded paymentsv1.WatchPaymentsRequest_Filter
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return err
	}
	listSynonymVersionsRequest := &searchv1.ListSynonymVersionsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSynonymVersionsRequest_Filter{
			Query:  (&decoded).GetQuery(),
			Region: req.GetFilter().GetRegion(),
		},
	}
	listSynonymVersionsReply, err := listSynonymVersionsCall(ctx, listSynonymVersionsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.search.v1.SynonymHistoryService/ListSynonymVersions")
	}
	exportRefundsStream, err := s.refundQueryClient.ExportRefunds(ctx, &paymentsv1.ExportRefundsRequest{
		Context: req.GetContext(),
		Filter: &paymentsv1.ExportRefundsRequest_Filter{
			Labels:       req.GetFilter().GetLabels(),
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.payments.v1.RefundQueryService/ExportRefunds")
	}
	for {
		received, err := exportRefundsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return grpcx.Wrap(err, "acme.payments.v1.RefundQueryService/ExportRefunds")
		}
		_ = received
	}
	if err := stream.Send(&paymentsv1.WatchPaymentsResponse{
		Result: &commonv1.OperationResult{
			Message: listSynonymVersionsReply.GetResult().GetMessage(),
			Status:  listSynonymVersionsReply.GetResult().GetStatus(),
		},
		Payment: &paymentsv1.Payment{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}); err != nil {
		return err
	}
	return nil
}

// BatchCreatePayments takes a stream of payments and answers once at the end.
func (s *PaymentServer) BatchCreatePayments(stream grpc.ClientStreamingServer[paymentsv1.BatchCreatePaymentsRequest, paymentsv1.BatchCreatePaymentsResponse]) error {
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
		s.log.InfoContext(ctx, "batch create payments", "tenant", msg.GetContext().GetTenantId())
		previewGiftCardRequest := &giftcardsv1.PreviewGiftCardRequest{
			Context: msg.GetContext(),
			GiftCard: &giftcardsv1.GiftCard{
				Audit: &commonv1.AuditInfo{UpdatedAt: msg.GetRecord().GetAudit().GetUpdatedAt()},
			},
		}
		if _, err := grpcx.Call(ctx, s.giftCardValidationClient.PreviewGiftCard, previewGiftCardRequest); err != nil {
			return grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/PreviewGiftCard")
		}
		explainRefundDecisionRequest := &paymentsv1.ExplainRefundDecisionRequest{Context: msg.GetContext()}
		if _, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*paymentsv1.ExplainRefundDecisionResponse, error) {
			return s.refundValidationClient.ExplainRefundDecision(ctx, explainRefundDecisionRequest)
		}); err != nil {
			return grpcx.Wrap(err, "acme.payments.v1.RefundValidationService/ExplainRefundDecision")
		}
		if _, err := s.searchQueryQueryClient.CountSearchQueries(ctx, &searchv1.CountSearchQueriesRequest{Context: msg.GetContext()}); err != nil {
			return grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/CountSearchQueries")
		}
		createLedgerAccountRequest := &ledgerv1.CreateLedgerAccountRequest{}
		createLedgerAccountRequest.Context = msg.GetContext()
		if _, err := s.ledgerAccountClient.CreateLedgerAccount(ctx, createLedgerAccountRequest); err != nil {
			return grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/CreateLedgerAccount")
		}
		accepted++
	}
	return stream.SendAndClose(&paymentsv1.BatchCreatePaymentsResponse{Accepted: accepted})
}

// ClonePayment creates a payment.
func (s *PaymentServer) ClonePayment(ctx context.Context, req *paymentsv1.ClonePaymentRequest) (*paymentsv1.ClonePaymentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "clone payment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	jobs := make(chan *paymentsv1.PaymentLine)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range jobs {
				if _, err := s.carrierRateCalculationClient.ExplainCarrierRateCalculation(workCtx, &shippingv1.ExplainCarrierRateCalculationRequest{
					Context: req.GetContext(),
					CarrierRate: &shippingv1.CarrierRate{
						Action:      &shippingv1.CarrierRate_Discount{Discount: &shippingv1.CarrierRateDiscountAction{Amount: line.GetAmount()}},
						Description: line.GetDescription(),
						Audit:       &commonv1.AuditInfo{CreatedAt: req.GetPayment().GetAudit().GetCreatedAt()},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetPayment().GetLines() {
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
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateCalculationService/ExplainCarrierRateCalculation")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*shippingv1.CompareTrackingEventOptionsResponse, len(req.GetPayment().GetLines()))
	for i, line2 := range req.GetPayment().GetLines() {
		group.Go(func() error {
			reply, err := s.trackingEventCalculationClient.CompareTrackingEventOptions(groupCtx, &shippingv1.CompareTrackingEventOptionsRequest{
				Context:       req.GetContext(),
				TrackingEvent: &shippingv1.TrackingEvent{Attributes: line2.GetAttributes()},
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventCalculationService/CompareTrackingEventOptions")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	scorePayoutRequest := &paymentsv1.ScorePayoutRequest{
		Context: req.GetContext(),
		Payout: &paymentsv1.Payout{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: req.GetPayment().GetBillingAddress().GetPostalCode(),
			},
		},
	}
	scorePayoutReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*paymentsv1.ScorePayoutResponse, error) {
		return s.payoutValidationClient.ScorePayout(ctx, scorePayoutRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutValidationService/ScorePayout")
	}
	return &paymentsv1.ClonePaymentResponse{
		Result: &commonv1.OperationResult{
			Details: scorePayoutReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Payment: &paymentsv1.Payment{
			PaymentId: req.GetPayment().GetPaymentId(),
			Status:    req.GetPayment().GetStatus(),
		},
	}, nil
}

// paymentServerCreatePaymentCreateLedgerAccountStep carries the request of one step of CreatePayment and, once it ran,
// its reply.
type paymentServerCreatePaymentCreateLedgerAccountStep struct {
	request *ledgerv1.CreateLedgerAccountRequest
	reply   *ledgerv1.CreateLedgerAccountResponse
}

func (s *PaymentServer) runCreatePaymentCreateLedgerAccount(ctx context.Context, step *paymentServerCreatePaymentCreateLedgerAccountStep) error {
	reply, err := s.ledgerAccountClient.CreateLedgerAccount(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// validateRefundSender returns a function that sends one request to
// acme.payments.v1.RefundValidationService/ValidateRefund.
func (s *PaymentServer) validateRefundSender(request *paymentsv1.ValidateRefundRequest) func(context.Context) (*paymentsv1.ValidateRefundResponse, error) {
	return func(ctx context.Context) (*paymentsv1.ValidateRefundResponse, error) {
		return s.refundValidationClient.ValidateRefund(ctx, request)
	}
}

// paymentValidationLazy dials acme.payments.v1.PaymentValidationService the first time a handler needs it.
func (s *PaymentServer) paymentValidationLazy() paymentsv1.PaymentValidationServiceClient {
	s.paymentValidationOnce.Do(func() { s.paymentValidationCached = paymentsv1.NewPaymentValidationServiceClient(s.conn) })
	return s.paymentValidationCached
}

// rewardCalculationLazy dials acme.loyalty.v1.RewardCalculationService the first time a handler needs it.
func (s *PaymentServer) rewardCalculationLazy() loyaltyv1.RewardCalculationServiceClient {
	s.rewardCalculationOnce.Do(func() { s.rewardCalculationCached = loyaltyv1.NewRewardCalculationServiceClient(s.conn) })
	return s.rewardCalculationCached
}

// paymentServerBatchGetPaymentsScoreReturnLabelStep carries the request of one step of BatchGetPayments and, once it
// ran, its reply.
type paymentServerBatchGetPaymentsScoreReturnLabelStep struct {
	request *returnsv1.ScoreReturnLabelRequest
	reply   *returnsv1.ScoreReturnLabelResponse
}

func (s *PaymentServer) runBatchGetPaymentsScoreReturnLabel(ctx context.Context, step *paymentServerBatchGetPaymentsScoreReturnLabelStep) error {
	reply, err := s.returnLabelValidationClient.ScoreReturnLabel(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
