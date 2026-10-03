package checkout

import (
	"context"
	"log/slog"

	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	deliveriesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/deliveries/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	patientsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/patients/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"google.golang.org/grpc"
)

// Register registers every service of the checkout domain on the server.
func Register(server *grpc.Server, deps platform.Deps) {
	wireCalls(deps)
	deps.Clients.Put("acme.customers.v1.HouseholdCalculationService", customersv1.NewHouseholdCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.metrics.v1.MetricSeriesQueryService", metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.patients.v1.PatientQueryService", patientsv1.NewPatientQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.search.v1.SynonymExportService", searchv1.NewSynonymExportServiceClient(deps.Conn))
	checkoutv1.RegisterCheckoutSessionServiceServer(server, NewCheckoutSessionServer(deps))
	checkoutv1.RegisterShippingQuoteQueryServiceServer(server, NewShippingQuoteQueryServer(deps))
	checkoutv1.RegisterPaymentIntentWorkflowServiceServer(server, NewPaymentIntentWorkflowServer(deps))
	checkoutv1.RegisterCheckoutSessionQueryServiceServer(server, NewCheckoutSessionQueryServer(deps))
	checkoutv1.RegisterShippingQuoteWorkflowServiceServer(server, NewShippingQuoteWorkflowServer(deps))
	checkoutv1.RegisterPaymentIntentApprovalServiceServer(server, NewPaymentIntentApprovalServer(deps))
	checkoutv1.RegisterCheckoutSessionWorkflowServiceServer(server, NewCheckoutSessionWorkflowServer(deps))
	checkoutv1.RegisterShippingQuoteApprovalServiceServer(server, NewShippingQuoteApprovalServer(deps))
	checkoutv1.RegisterPaymentIntentValidationServiceServer(server, NewPaymentIntentValidationServer(deps))
	checkoutv1.RegisterCheckoutSessionApprovalServiceServer(server, NewCheckoutSessionApprovalServer(deps))
}

// The functions that call other services, which wireCalls sets when the module registers.
var (
	resolveShippingQuotesCall func(context.Context, *checkoutv1.ResolveShippingQuotesRequest) (*checkoutv1.ResolveShippingQuotesResponse, error)
	scoreRefundCall           func(context.Context, *paymentsv1.ScoreRefundRequest, ...grpc.CallOption) (*paymentsv1.ScoreRefundResponse, error)
)

// wireCalls points the package's call functions at the services they reach.
func wireCalls(deps platform.Deps) {
	shippingQuoteQueryForResolveShippingQuotes := checkoutv1.NewShippingQuoteQueryServiceClient(deps.Conn)
	resolveShippingQuotesCall = func(ctx context.Context, request *checkoutv1.ResolveShippingQuotesRequest) (*checkoutv1.ResolveShippingQuotesResponse, error) {
		return shippingQuoteQueryForResolveShippingQuotes.ResolveShippingQuotes(ctx, request)
	}
	refundValidationForScoreRefund := paymentsv1.NewRefundValidationServiceClient(deps.Conn)
	scoreRefundCall = refundValidationForScoreRefund.ScoreRefund
}

// workers make the calls the domain's events ask for, apart from the requests that published them.
type workers struct {
	log                                   *slog.Logger
	sellerPayoutScheduleCalculationClient sellersv1.SellerPayoutScheduleCalculationServiceClient
	deliveryClient                        deliveriesv1.DeliveryServiceClient
	loyaltyMemberApprovalClient           loyaltyv1.LoyaltyMemberApprovalServiceClient
}

// StartWorkers subscribes the domain's workers to their topics until the context ends.
func StartWorkers(ctx context.Context, deps platform.Deps) {
	w := &workers{
		log:                                   deps.Logger,
		sellerPayoutScheduleCalculationClient: sellersv1.NewSellerPayoutScheduleCalculationServiceClient(deps.Conn),
		deliveryClient:                        deliveriesv1.NewDeliveryServiceClient(deps.Conn),
		loyaltyMemberApprovalClient:           loyaltyv1.NewLoyaltyMemberApprovalServiceClient(deps.Conn),
	}
	go w.consume(ctx, "checkout.browse_checkout_sessions.simulate_seller_payout_schedule", deps.Bus.Subscribe("checkout.browse_checkout_sessions.simulate_seller_payout_schedule"), w.handleCheckoutBrowseCheckoutSessionsSimulateSellerPayoutSchedule)
	go w.consume(ctx, "checkout.explain_payment_intent_decision.create_delivery", deps.Bus.Subscribe("checkout.explain_payment_intent_decision.create_delivery"), w.handleCheckoutExplainPaymentIntentDecisionCreateDelivery)
	go w.consume(ctx, "checkout.recall_checkout_session.submit_loyalty_member", deps.Bus.Subscribe("checkout.recall_checkout_session.submit_loyalty_member"), w.handleCheckoutRecallCheckoutSessionSubmitLoyaltyMember)
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

// handleCheckoutBrowseCheckoutSessionsSimulateSellerPayoutSchedule makes the call the
// checkout.browse_checkout_sessions.simulate_seller_payout_schedule event asks for.
func (w *workers) handleCheckoutBrowseCheckoutSessionsSimulateSellerPayoutSchedule(ctx context.Context, event eventbus.Event) error {
	_, err := w.sellerPayoutScheduleCalculationClient.SimulateSellerPayoutSchedule(ctx, &sellersv1.SimulateSellerPayoutScheduleRequest{
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Location: &commonv1.PostalAddress{
				Region: event.Attributes["seller_payout_schedule.location.region"],
			},
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					Email: event.Attributes["seller_payout_schedule.participants.contact.email"],
				},
			}},
			Organizer: &identityv1.UserRef{
				UserId: event.Attributes["seller_payout_schedule.organizer.user_id"],
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

// handleCheckoutExplainPaymentIntentDecisionCreateDelivery makes the call the
// checkout.explain_payment_intent_decision.create_delivery event asks for.
func (w *workers) handleCheckoutExplainPaymentIntentDecisionCreateDelivery(ctx context.Context, event eventbus.Event) error {
	_, err := w.deliveryClient.CreateDelivery(ctx, &deliveriesv1.CreateDeliveryRequest{
		Delivery: &deliveriesv1.Delivery{
			Audit: &commonv1.AuditInfo{CreatedBy: event.Attributes["delivery.audit.created_by"]},
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

// handleCheckoutRecallCheckoutSessionSubmitLoyaltyMember makes the call the
// checkout.recall_checkout_session.submit_loyalty_member event asks for.
func (w *workers) handleCheckoutRecallCheckoutSessionSubmitLoyaltyMember(ctx context.Context, event eventbus.Event) error {
	_, err := w.loyaltyMemberApprovalClient.SubmitLoyaltyMember(ctx, &loyaltyv1.SubmitLoyaltyMemberRequest{
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Funding: &loyaltyv1.LoyaltyMember_Card{Card: &loyaltyv1.LoyaltyMemberCardFunding{
				HolderName: event.Attributes["loyalty_member.card.holder_name"],
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: event.Attributes["loyalty_member.audit.updated_by"],
			},
		},
		Details: &loyaltyv1.SubmitLoyaltyMemberRequest_Details{
			Approver: &identityv1.UserRef{Email: event.Attributes["details.approver.email"]},
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
