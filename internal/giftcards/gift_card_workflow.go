package giftcards

import (
	"context"
	"log/slog"
	"sync"

	broadcastsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/broadcasts/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	yardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/yards/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// GiftCardWorkflowServer implements `acme.giftcards.v1.GiftCardWorkflowService`.
// Runs the long-lived workflow that takes a gift card from start to finish.
type GiftCardWorkflowServer struct {
	giftcardsv1.UnimplementedGiftCardWorkflowServiceServer
	store                           *Store
	log                             *slog.Logger
	conn                            *grpc.ClientConn
	priceQuoteQueryOnce             sync.Once
	priceQuoteQueryCached           pricingv1.PriceQuoteQueryServiceClient
	bus                             *eventbus.Bus
	refundCalculationClient         paymentsv1.RefundCalculationServiceClient
	broadcastQueryClient            broadcastsv1.BroadcastQueryServiceClient
	giftCardClient                  giftcardsv1.GiftCardServiceClient
	giftCardQueryClient             giftcardsv1.GiftCardQueryServiceClient
	apiCredentialNotificationClient identityv1.ApiCredentialNotificationServiceClient
	giftCardApprovalClient          giftcardsv1.GiftCardApprovalServiceClient
	yardCalculationClient           yardsv1.YardCalculationServiceClient
}

// NewGiftCardWorkflowServer wires a GiftCardWorkflowServer.
func NewGiftCardWorkflowServer(deps platform.Deps) *GiftCardWorkflowServer {
	return &GiftCardWorkflowServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.giftcards.v1.GiftCardWorkflowService"),
		conn:                            deps.Conn,
		bus:                             deps.Bus,
		refundCalculationClient:         paymentsv1.NewRefundCalculationServiceClient(deps.Conn),
		broadcastQueryClient:            broadcastsv1.NewBroadcastQueryServiceClient(deps.Conn),
		giftCardClient:                  giftcardsv1.NewGiftCardServiceClient(deps.Conn),
		giftCardQueryClient:             giftcardsv1.NewGiftCardQueryServiceClient(deps.Conn),
		apiCredentialNotificationClient: identityv1.NewApiCredentialNotificationServiceClient(deps.Conn),
		giftCardApprovalClient:          giftcardsv1.NewGiftCardApprovalServiceClient(deps.Conn),
		yardCalculationClient:           yardsv1.NewYardCalculationServiceClient(deps.Conn),
	}
}

// StartGiftCardWorkflow starts a gift card.
func (s *GiftCardWorkflowServer) StartGiftCardWorkflow(ctx context.Context, req *giftcardsv1.StartGiftCardWorkflowRequest) (*giftcardsv1.StartGiftCardWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveGiftCard(ctx, req.GetContext().GetTenantId(), req.GetGiftCard()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card")
	}
	s.log.InfoContext(ctx, "start gift card workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCard().GetAudit().GetEtag())
	labels := make(map[string]string, len(req.GetGiftCard().GetLabels()))
	for key, value := range req.GetGiftCard().GetLabels() {
		labels[key] = value
	}
	findPriceQuotesRequest := &pricingv1.FindPriceQuotesRequest{
		Context: req.GetContext(),
		Filter: &pricingv1.FindPriceQuotesRequest_Filter{
			Labels:       labels,
			CreatedAfter: req.GetGiftCard().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetGiftCardId(),
		},
	}
	findPriceQuotesReply, err := s.priceQuoteQueryLazy().FindPriceQuotes(ctx, findPriceQuotesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteQueryService/FindPriceQuotes")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "giftcards.start_gift_card_workflow.score_redemption",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"redemption.subject.display_name": req.GetGiftCard().GetHolder().GetDisplayName(),
			"redemption.kind":                 req.GetGiftCard().GetHolder().GetKind(),
			"context.tenant_id":               req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &giftcardsv1.StartGiftCardWorkflowResponse{
		Outcome: &giftcardsv1.StartGiftCardWorkflowResponse_Outcome{Status: findPriceQuotesReply.GetResult().GetStatus()},
		GiftCard: &giftcardsv1.GiftCard{
			GiftCardId: req.GetGiftCardId(),
			Available:  req.GetGiftCard().GetAvailable(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// AdvanceGiftCardWorkflow advances a gift card.
func (s *GiftCardWorkflowServer) AdvanceGiftCardWorkflow(ctx context.Context, req *giftcardsv1.AdvanceGiftCardWorkflowRequest) (*giftcardsv1.AdvanceGiftCardWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveGiftCard(ctx, req.GetContext().GetTenantId(), req.GetGiftCard()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card")
	}
	s.log.InfoContext(ctx, "advance gift card workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCard().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetGiftCard())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded giftcardsv1.GiftCard
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	calculateRefundReply, err := s.refundCalculationClient.CalculateRefund(ctx, &paymentsv1.CalculateRefundRequest{
		Context: req.GetContext(),
		Refund: &paymentsv1.Refund{
			Settlement:   &paymentsv1.Refund_Card{Card: &paymentsv1.RefundCardSettlement{Token: req.GetGiftCard().GetCard().GetToken()}},
			Counterparty: &customersv1.CustomerRef{DisplayName: (&decoded).GetHolder().GetDisplayName()},
			Audit:        &commonv1.AuditInfo{CreatedAt: req.GetGiftCard().GetAudit().GetCreatedAt()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundCalculationService/CalculateRefund")
	}
	return &giftcardsv1.AdvanceGiftCardWorkflowResponse{
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{
				CreatedBy: calculateRefundReply.GetRefund().GetAudit().GetCreatedBy(),
			},
			GiftCardId: req.GetGiftCardId(),
			Holder:     req.GetGiftCard().GetHolder(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CancelGiftCardWorkflow cancels a gift card.
func (s *GiftCardWorkflowServer) CancelGiftCardWorkflow(ctx context.Context, req *giftcardsv1.CancelGiftCardWorkflowRequest) (*giftcardsv1.CancelGiftCardWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CancelGiftCardWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveGiftCard(ctx, req.GetContext().GetTenantId(), req.GetGiftCard()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card")
	}
	s.log.InfoContext(ctx, "cancel gift card workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCard().GetAudit().GetEtag())
	resolveBroadcastsRequest := buildResolveBroadcastsRequestForCancelGiftCardWorkflow(req)
	resolveBroadcastsReply, err := s.broadcastQueryClient.ResolveBroadcasts(ctx, resolveBroadcastsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.broadcasts.v1.BroadcastQueryService/ResolveBroadcasts")
	}
	return &giftcardsv1.CancelGiftCardWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: resolveBroadcastsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		GiftCard: &giftcardsv1.GiftCard{
			GiftCardId: req.GetGiftCardId(),
			Labels:     req.GetGiftCard().GetLabels(),
		},
	}, nil
}

// RetryGiftCardWorkflow retrys a gift card.
func (s *GiftCardWorkflowServer) RetryGiftCardWorkflow(ctx context.Context, req *giftcardsv1.RetryGiftCardWorkflowRequest) (*giftcardsv1.RetryGiftCardWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RetryGiftCardWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveGiftCard(ctx, req.GetContext().GetTenantId(), req.GetGiftCard()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card")
	}
	s.log.InfoContext(ctx, "retry gift card workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCard().GetAudit().GetEtag())
	deleteGiftCardRequest := &giftcardsv1.DeleteGiftCardRequest{}
	deleteGiftCardRequest.Context = req.GetContext()
	deleteGiftCardRequest.GiftCardId = req.GetGiftCardId()
	deleteGiftCardRequest.Etag = req.GetGiftCard().GetAudit().GetEtag()
	deleteGiftCardRequest.Reason = req.GetReason()
	deleteGiftCardReply, err := s.giftCardClient.DeleteGiftCard(ctx, deleteGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardService/DeleteGiftCard")
	}
	return &giftcardsv1.RetryGiftCardWorkflowResponse{
		Outcome: &giftcardsv1.RetryGiftCardWorkflowResponse_Outcome{Message: deleteGiftCardReply.GetResult().GetMessage()},
		GiftCard: &giftcardsv1.GiftCard{
			GiftCardId: req.GetGiftCardId(),
			Status:     req.GetGiftCard().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PauseGiftCardWorkflow pauses a gift card.
func (s *GiftCardWorkflowServer) PauseGiftCardWorkflow(ctx context.Context, req *giftcardsv1.PauseGiftCardWorkflowRequest) (*giftcardsv1.PauseGiftCardWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PauseGiftCardWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveGiftCard(ctx, req.GetContext().GetTenantId(), req.GetGiftCard()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card")
	}
	s.log.InfoContext(ctx, "pause gift card workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCard().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "gift_card_transaction_id", req.GetGiftCard().GetHolder().GetGiftCardTransactionId())
	findGiftCardsReply, err := s.giftCardQueryClient.FindGiftCards(ctx, &giftcardsv1.FindGiftCardsRequest{
		Context: req.GetContext(),
		Filter: &giftcardsv1.FindGiftCardsRequest_Filter{
			Labels:       req.GetGiftCard().GetLabels(),
			CreatedAfter: req.GetGiftCard().GetAudit().GetUpdatedAt(),
			OwnerId:      tenancy.Value(ctx, "gift_card_transaction_id"),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardQueryService/FindGiftCards")
	}
	return &giftcardsv1.PauseGiftCardWorkflowResponse{
		Result: &commonv1.OperationResult{Status: findGiftCardsReply.GetResult().GetStatus()},
		GiftCard: &giftcardsv1.GiftCard{
			GiftCardId: req.GetGiftCardId(),
			Labels:     req.GetGiftCard().GetLabels(),
		},
	}, nil
}

// InspectGiftCardWorkflow returns a gift card.
func (s *GiftCardWorkflowServer) InspectGiftCardWorkflow(ctx context.Context, req *giftcardsv1.InspectGiftCardWorkflowRequest) (*giftcardsv1.InspectGiftCardWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected InspectGiftCardWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "gift_card", req.GetGiftCardId(), "inspect_gift_card_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect gift card workflow", "tenant", req.GetContext().GetTenantId())
	subscribeApiCredentialUpdatesRequest := buildSubscribeApiCredentialUpdatesRequestForInspectGiftCardWorkflow(req)
	subscribeApiCredentialUpdatesReply, err := s.apiCredentialNotificationClient.SubscribeApiCredentialUpdates(ctx, subscribeApiCredentialUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/SubscribeApiCredentialUpdates")
	}
	return &giftcardsv1.InspectGiftCardWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status:  subscribeApiCredentialUpdatesReply.GetOutcome().GetStatus(),
			Message: subscribeApiCredentialUpdatesReply.GetResult().GetMessage(),
		},
		GiftCard: &giftcardsv1.GiftCard{GiftCardId: req.GetGiftCardId()},
	}, nil
}

// SignalGiftCardWorkflow signals a gift card.
func (s *GiftCardWorkflowServer) SignalGiftCardWorkflow(ctx context.Context, req *giftcardsv1.SignalGiftCardWorkflowRequest) (*giftcardsv1.SignalGiftCardWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveGiftCard(ctx, req.GetContext().GetTenantId(), req.GetGiftCard()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card")
	}
	s.log.InfoContext(ctx, "signal gift card workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCard().GetAudit().GetEtag())
	for limit := range iterx.Each(req.GetGiftCard().GetLimits(), func(candidate *giftcardsv1.GiftCardLimit) bool { return candidate != nil }) {
		if _, err := s.giftCardApprovalClient.RecallGiftCard(ctx, &giftcardsv1.RecallGiftCardRequest{
			Context: req.GetContext(),
			GiftCard: &giftcardsv1.GiftCard{
				Limits: []*giftcardsv1.GiftCardLimit{&giftcardsv1.GiftCardLimit{
					Name:   limit.GetName(),
					Amount: limit.GetAmount(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardApprovalService/RecallGiftCard")
		}
	}
	quoteYardRequest := &yardsv1.QuoteYardRequest{Context: req.GetContext()}
	type quoteYardResult struct {
		reply *yardsv1.QuoteYardResponse
		err   error
	}
	quoteYardResults := make(chan quoteYardResult, 1)
	go func() {
		reply, err := s.yardCalculationClient.QuoteYard(ctx, quoteYardRequest)
		quoteYardResults <- quoteYardResult{reply, err}
	}()
	var quoteYardReply *yardsv1.QuoteYardResponse
	select {
	case result := <-quoteYardResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.yards.v1.YardCalculationService/QuoteYard")
		}
		quoteYardReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &giftcardsv1.SignalGiftCardWorkflowResponse{
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{Etag: quoteYardReply.GetYard().GetAudit().GetEtag()},
			Holder: &giftcardsv1.GiftCardTransactionRef{
				DisplayName: quoteYardReply.GetYard().GetOwner().GetDisplayName(),
			},
			GiftCardId:   req.GetGiftCardId(),
			CurrencyCode: req.GetGiftCard().GetCurrencyCode(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// priceQuoteQueryLazy dials acme.pricing.v1.PriceQuoteQueryService the first time a handler needs it.
func (s *GiftCardWorkflowServer) priceQuoteQueryLazy() pricingv1.PriceQuoteQueryServiceClient {
	s.priceQuoteQueryOnce.Do(func() { s.priceQuoteQueryCached = pricingv1.NewPriceQuoteQueryServiceClient(s.conn) })
	return s.priceQuoteQueryCached
}
