package giftcards

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	procurementv1 "github.com/hounddogai/protomap-test-golang/gen/acme/procurement/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"google.golang.org/grpc"
)

// Register registers every service of the giftcards domain on the server.
func Register(server *grpc.Server, deps platform.Deps) {
	wireCalls(deps)
	deps.Clients.Put("acme.giftcards.v1.GiftCardBatchApprovalService", giftcardsv1.NewGiftCardBatchApprovalServiceClient(deps.Conn))
	deps.Clients.Put("acme.giftcards.v1.GiftCardBatchCalculationService", giftcardsv1.NewGiftCardBatchCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.giftcards.v1.GiftCardBatchValidationService", giftcardsv1.NewGiftCardBatchValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.giftcards.v1.GiftCardQueryService", giftcardsv1.NewGiftCardQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.giftcards.v1.GiftCardTransactionApprovalService", giftcardsv1.NewGiftCardTransactionApprovalServiceClient(deps.Conn))
	deps.Clients.Put("acme.giftcards.v1.GiftCardTransactionValidationService", giftcardsv1.NewGiftCardTransactionValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.giftcards.v1.GiftCardValidationService", giftcardsv1.NewGiftCardValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.identity.v1.SessionHistoryService", identityv1.NewSessionHistoryServiceClient(deps.Conn))
	deps.Clients.Put("acme.notifications.v1.NotificationValidationService", notificationsv1.NewNotificationValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.procurement.v1.PurchaseRequisitionQueryService", procurementv1.NewPurchaseRequisitionQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.search.v1.SearchIndexNotificationService", searchv1.NewSearchIndexNotificationServiceClient(deps.Conn))
	deps.Clients.Put("acme.search.v1.SynonymSyncService", searchv1.NewSynonymSyncServiceClient(deps.Conn))
	giftcardsv1.RegisterGiftCardServiceServer(server, NewGiftCardServer(deps))
	giftcardsv1.RegisterGiftCardBatchQueryServiceServer(server, NewGiftCardBatchQueryServer(deps))
	giftcardsv1.RegisterGiftCardTransactionWorkflowServiceServer(server, NewGiftCardTransactionWorkflowServer(deps))
	giftcardsv1.RegisterGiftCardQueryServiceServer(server, NewGiftCardQueryServer(deps))
	giftcardsv1.RegisterGiftCardBatchWorkflowServiceServer(server, NewGiftCardBatchWorkflowServer(deps))
	giftCardTransactionApprovalServer := NewGiftCardTransactionApprovalServer(deps)
	server.RegisterService(&giftcardsv1.GiftCardTransactionApprovalService_ServiceDesc, giftCardTransactionApprovalServer)
	giftcardsv1.RegisterGiftCardWorkflowServiceServer(server, NewGiftCardWorkflowServer(deps))
	giftcardsv1.RegisterGiftCardBatchApprovalServiceServer(server, NewGiftCardBatchApprovalServer(deps))
	giftcardsv1.RegisterGiftCardTransactionValidationServiceServer(server, NewGiftCardTransactionValidationServer(deps))
	giftcardsv1.RegisterGiftCardApprovalServiceServer(server, NewGiftCardApprovalServer(deps))
	giftcardsv1.RegisterGiftCardBatchValidationServiceServer(server, NewGiftCardBatchValidationServer(deps))
	giftcardsv1.RegisterGiftCardTransactionCalculationServiceServer(server, NewGiftCardTransactionCalculationServer(deps))
	giftcardsv1.RegisterGiftCardValidationServiceServer(server, NewGiftCardValidationServer(deps))
	giftcardsv1.RegisterGiftCardBatchCalculationServiceServer(server, NewGiftCardBatchCalculationServer(deps))
	giftcardsv1.RegisterGiftCardTransactionLifecycleServiceServer(server, NewGiftCardTransactionLifecycleServer(deps))
	giftcardsv1.RegisterGiftCardCalculationServiceServer(server, NewGiftCardCalculationServer(deps))
}

// The functions that call other services, which wireCalls sets when the module registers.
var (
	previewGiftCardCall        func(context.Context, *giftcardsv1.PreviewGiftCardRequest, ...grpc.CallOption) (*giftcardsv1.PreviewGiftCardResponse, error)
	resolveGiftCardBatchesCall func(context.Context, *giftcardsv1.ResolveGiftCardBatchesRequest, ...grpc.CallOption) (*giftcardsv1.ResolveGiftCardBatchesResponse, error)
	normalizeLedgerAccountCall func(context.Context, *ledgerv1.NormalizeLedgerAccountRequest) (*ledgerv1.NormalizeLedgerAccountResponse, error)
	validateSynonymCall        func(context.Context, *searchv1.ValidateSynonymRequest) (*searchv1.ValidateSynonymResponse, error)
	quoteGiftCardBatchCall     func(context.Context, *giftcardsv1.QuoteGiftCardBatchRequest) (*giftcardsv1.QuoteGiftCardBatchResponse, error)
)

// wireCalls points the package's call functions at the services they reach.
func wireCalls(deps platform.Deps) {
	giftCardValidationForPreviewGiftCard := giftcardsv1.NewGiftCardValidationServiceClient(deps.Conn)
	previewGiftCardCall = giftCardValidationForPreviewGiftCard.PreviewGiftCard
	giftCardBatchQueryForResolveGiftCardBatches := giftcardsv1.NewGiftCardBatchQueryServiceClient(deps.Conn)
	resolveGiftCardBatchesCall = giftCardBatchQueryForResolveGiftCardBatches.ResolveGiftCardBatches
	ledgerAccountValidationForNormalizeLedgerAccount := ledgerv1.NewLedgerAccountValidationServiceClient(deps.Conn)
	normalizeLedgerAccountCall = func(ctx context.Context, request *ledgerv1.NormalizeLedgerAccountRequest) (*ledgerv1.NormalizeLedgerAccountResponse, error) {
		return ledgerAccountValidationForNormalizeLedgerAccount.NormalizeLedgerAccount(ctx, request)
	}
	synonymValidationForValidateSynonym := searchv1.NewSynonymValidationServiceClient(deps.Conn)
	validateSynonymCall = func(ctx context.Context, request *searchv1.ValidateSynonymRequest) (*searchv1.ValidateSynonymResponse, error) {
		return synonymValidationForValidateSynonym.ValidateSynonym(ctx, request)
	}
	giftCardBatchCalculationForQuoteGiftCardBatch := giftcardsv1.NewGiftCardBatchCalculationServiceClient(deps.Conn)
	quoteGiftCardBatchCall = func(ctx context.Context, request *giftcardsv1.QuoteGiftCardBatchRequest) (*giftcardsv1.QuoteGiftCardBatchResponse, error) {
		return giftCardBatchCalculationForQuoteGiftCardBatch.QuoteGiftCardBatch(ctx, request)
	}
}

// workers make the calls the domain's events ask for, apart from the requests that published them.
type workers struct {
	log                          *slog.Logger
	giftCardApprovalClient       giftcardsv1.GiftCardApprovalServiceClient
	redemptionValidationClient   promotionsv1.RedemptionValidationServiceClient
	metricSeriesValidationClient metricsv1.MetricSeriesValidationServiceClient
	giftCardValidationClient     giftcardsv1.GiftCardValidationServiceClient
}

// StartWorkers subscribes the domain's workers to their topics until the context ends.
func StartWorkers(ctx context.Context, deps platform.Deps) {
	w := &workers{
		log:                          deps.Logger,
		giftCardApprovalClient:       giftcardsv1.NewGiftCardApprovalServiceClient(deps.Conn),
		redemptionValidationClient:   promotionsv1.NewRedemptionValidationServiceClient(deps.Conn),
		metricSeriesValidationClient: metricsv1.NewMetricSeriesValidationServiceClient(deps.Conn),
		giftCardValidationClient:     giftcardsv1.NewGiftCardValidationServiceClient(deps.Conn),
	}
	go w.consume(ctx, "giftcards.inspect_gift_card_transaction_workflow.list_pending_gift_cards", deps.Bus.Subscribe("giftcards.inspect_gift_card_transaction_workflow.list_pending_gift_cards"), w.handleGiftcardsInspectGiftCardTransactionWorkflowListPendingGiftCards)
	go w.consume(ctx, "giftcards.start_gift_card_workflow.score_redemption", deps.Bus.Subscribe("giftcards.start_gift_card_workflow.score_redemption"), w.handleGiftcardsStartGiftCardWorkflowScoreRedemption)
	go w.consume(ctx, "giftcards.validate_gift_card_transaction.explain_metric_series_decision", deps.Bus.Subscribe("giftcards.validate_gift_card_transaction.explain_metric_series_decision"), w.handleGiftcardsValidateGiftCardTransactionExplainMetricSeriesDecision)
	go w.consume(ctx, "giftcards.normalize_gift_card_batch.validate_gift_card", deps.Bus.Subscribe("giftcards.normalize_gift_card_batch.validate_gift_card"), w.handleGiftcardsNormalizeGiftCardBatchValidateGiftCard)
	go w.consume(ctx, "giftcards.estimate_gift_card.normalize_metric_series", deps.Bus.Subscribe("giftcards.estimate_gift_card.normalize_metric_series"), w.handleGiftcardsEstimateGiftCardNormalizeMetricSeries)
}

func (w *workers) consume(ctx context.Context, topic string, events <-chan eventbus.Event, handle func(context.Context, eventbus.Event) error) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-events:
			if err := handle(ctx, event); err != nil {
				w.log.ErrorContext(ctx, "event failed", "topic", topic, "error", err)
			}
		}
	}
}

// handleGiftcardsInspectGiftCardTransactionWorkflowListPendingGiftCards makes the call the
// giftcards.inspect_gift_card_transaction_workflow.list_pending_gift_cards event asks for.
func (w *workers) handleGiftcardsInspectGiftCardTransactionWorkflowListPendingGiftCards(ctx context.Context, event eventbus.Event) error {
	_, err := w.giftCardApprovalClient.ListPendingGiftCards(ctx, &giftcardsv1.ListPendingGiftCardsRequest{
		Filter: &giftcardsv1.ListPendingGiftCardsRequest_Filter{OwnerId: event.Attributes["filter.owner_id"]},
		Context: &commonv1.RequestContext{
			TenantId: event.Attributes["context.tenant_id"],
			Actor: &commonv1.Actor{
				UserId:         "monolith",
				ServiceAccount: true,
			},
		},
	})
	return err
}

// handleGiftcardsStartGiftCardWorkflowScoreRedemption makes the call the
// giftcards.start_gift_card_workflow.score_redemption event asks for.
func (w *workers) handleGiftcardsStartGiftCardWorkflowScoreRedemption(ctx context.Context, event eventbus.Event) error {
	_, err := w.redemptionValidationClient.ScoreRedemption(ctx, &promotionsv1.ScoreRedemptionRequest{
		Redemption: &promotionsv1.Redemption{
			Subject: &promotionsv1.CouponRef{
				DisplayName: event.Attributes["redemption.subject.display_name"],
			},
			Kind: event.Attributes["redemption.kind"],
		},
		Context: &commonv1.RequestContext{
			TenantId: event.Attributes["context.tenant_id"],
			Actor: &commonv1.Actor{
				UserId:         "monolith",
				ServiceAccount: true,
			},
		},
	})
	return err
}

// handleGiftcardsValidateGiftCardTransactionExplainMetricSeriesDecision makes the call the
// giftcards.validate_gift_card_transaction.explain_metric_series_decision event asks for.
func (w *workers) handleGiftcardsValidateGiftCardTransactionExplainMetricSeriesDecision(ctx context.Context, event eventbus.Event) error {
	_, err := w.metricSeriesValidationClient.ExplainMetricSeriesDecision(ctx, &metricsv1.ExplainMetricSeriesDecisionRequest{
		Details: &metricsv1.ExplainMetricSeriesDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: event.Attributes["details.contact.email"]},
		},
		Context: &commonv1.RequestContext{
			TenantId: event.Attributes["context.tenant_id"],
			Actor: &commonv1.Actor{
				UserId:         "monolith",
				ServiceAccount: true,
			},
		},
	})
	return err
}

// handleGiftcardsNormalizeGiftCardBatchValidateGiftCard makes the call the
// giftcards.normalize_gift_card_batch.validate_gift_card event asks for.
func (w *workers) handleGiftcardsNormalizeGiftCardBatchValidateGiftCard(ctx context.Context, event eventbus.Event) error {
	_, err := w.giftCardValidationClient.ValidateGiftCard(ctx, &giftcardsv1.ValidateGiftCardRequest{
		GiftCard: &giftcardsv1.GiftCard{
			Holder: &giftcardsv1.GiftCardTransactionRef{
				DisplayName: event.Attributes["gift_card.holder.display_name"],
			},
			Funding: &giftcardsv1.GiftCard_Bank{Bank: &giftcardsv1.GiftCardBankFunding{
				AccountHolder: event.Attributes["gift_card.bank.account_holder"],
			}},
			Audit: &commonv1.AuditInfo{Etag: event.Attributes["gift_card.audit.etag"]},
		},
		Context: &commonv1.RequestContext{
			TenantId: event.Attributes["context.tenant_id"],
			Actor: &commonv1.Actor{
				UserId:         "monolith",
				ServiceAccount: true,
			},
		},
	})
	return err
}

// handleGiftcardsEstimateGiftCardNormalizeMetricSeries makes the call the
// giftcards.estimate_gift_card.normalize_metric_series event asks for.
func (w *workers) handleGiftcardsEstimateGiftCardNormalizeMetricSeries(ctx context.Context, event eventbus.Event) error {
	_, err := w.metricSeriesValidationClient.NormalizeMetricSeries(ctx, &metricsv1.NormalizeMetricSeriesRequest{
		MetricSeries: &metricsv1.MetricSeries{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: event.Attributes["metric_series.audit.updated_by"],
			},
		},
		Context: &commonv1.RequestContext{
			TenantId: event.Attributes["context.tenant_id"],
			Actor: &commonv1.Actor{
				UserId:         "monolith",
				ServiceAccount: true,
			},
		},
	})
	return err
}
