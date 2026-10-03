package payments

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	driversv1 "github.com/hounddogai/protomap-test-golang/gen/acme/drivers/v1"
	forecastingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/forecasting/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// PaymentInstrumentValidationServer implements `acme.payments.v1.PaymentInstrumentValidationService`.
// Checks payment instruments against the rules before anything acts on them.
type PaymentInstrumentValidationServer struct {
	paymentsv1.UnimplementedPaymentInstrumentValidationServiceServer
	store                               *Store
	log                                 *slog.Logger
	payoutCalculationClient             paymentsv1.PayoutCalculationServiceClient
	sellerValidationClient              sellersv1.SellerValidationServiceClient
	fraudSignalQueryClient              fraudv1.FraudSignalQueryServiceClient
	clients                             *registry.Registry
	shipmentQueryClient                 shippingv1.ShipmentQueryServiceClient
	tenantAdminClient                   identityv1.TenantAdminServiceClient
	giftCardBatchQueryClient            giftcardsv1.GiftCardBatchQueryServiceClient
	conn                                *grpc.ClientConn
	giftCardTransactionValidationOnce   sync.Once
	giftCardTransactionValidationCached giftcardsv1.GiftCardTransactionValidationServiceClient
	accessPolicyQueryClient             authorizationv1.AccessPolicyQueryServiceClient
	metricSeriesPort                    metricSeriesPort
	paymentInstrumentCalculationClient  paymentsv1.PaymentInstrumentCalculationServiceClient
	searchQueryValidationOnce           sync.Once
	searchQueryValidationCached         searchv1.SearchQueryValidationServiceClient
	demandForecastQueryClient           forecastingv1.DemandForecastQueryServiceClient
}

// NewPaymentInstrumentValidationServer wires a PaymentInstrumentValidationServer.
func NewPaymentInstrumentValidationServer(deps platform.Deps) *PaymentInstrumentValidationServer {
	return &PaymentInstrumentValidationServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.payments.v1.PaymentInstrumentValidationService"),
		payoutCalculationClient:            paymentsv1.NewPayoutCalculationServiceClient(deps.Conn),
		sellerValidationClient:             sellersv1.NewSellerValidationServiceClient(deps.Conn),
		fraudSignalQueryClient:             fraudv1.NewFraudSignalQueryServiceClient(deps.Conn),
		clients:                            deps.Clients,
		shipmentQueryClient:                shippingv1.NewShipmentQueryServiceClient(deps.Conn),
		tenantAdminClient:                  identityv1.NewTenantAdminServiceClient(deps.Conn),
		giftCardBatchQueryClient:           giftcardsv1.NewGiftCardBatchQueryServiceClient(deps.Conn),
		conn:                               deps.Conn,
		accessPolicyQueryClient:            authorizationv1.NewAccessPolicyQueryServiceClient(deps.Conn),
		metricSeriesPort:                   newMetricSeriesPort(deps),
		paymentInstrumentCalculationClient: paymentsv1.NewPaymentInstrumentCalculationServiceClient(deps.Conn),
		demandForecastQueryClient:          forecastingv1.NewDemandForecastQueryServiceClient(deps.Conn),
	}
}

// ValidatePaymentInstrument validates a payment instrument.
func (s *PaymentInstrumentValidationServer) ValidatePaymentInstrument(ctx context.Context, req *paymentsv1.ValidatePaymentInstrumentRequest) (*paymentsv1.ValidatePaymentInstrumentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "validate payment instrument", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*paymentsv1.ExplainPayoutCalculationResponse, len(req.GetPaymentInstrument().GetLimits()))
	for i, limit := range req.GetPaymentInstrument().GetLimits() {
		group.Go(func() error {
			reply, err := s.payoutCalculationClient.ExplainPayoutCalculation(groupCtx, &paymentsv1.ExplainPayoutCalculationRequest{
				Context: req.GetContext(),
				Payout: &paymentsv1.Payout{
					Lines:      []*paymentsv1.PayoutLine{&paymentsv1.PayoutLine{Amount: limit.GetAmount()}},
					Settlement: &paymentsv1.Payout_Card{Card: &paymentsv1.PayoutCardSettlement{Token: req.GetPaymentInstrument().GetCard().GetToken()}},
					Counterparty: &paymentsv1.RefundRef{
						DisplayName: req.GetPaymentInstrument().GetHolder().GetDisplayName(),
					},
				},
				Details: &paymentsv1.ExplainPayoutCalculationRequest_Details{
					Contact: &commonv1.ContactInfo{Phone: req.GetPaymentInstrument().GetContact().GetPhone()},
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
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutCalculationService/ExplainPayoutCalculation")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &paymentsv1.ValidatePaymentInstrumentResponse{
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			Holder:              req.GetPaymentInstrument().GetHolder(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PreviewPaymentInstrument previews a payment instrument.
func (s *PaymentInstrumentValidationServer) PreviewPaymentInstrument(ctx context.Context, req *paymentsv1.PreviewPaymentInstrumentRequest) (*paymentsv1.PreviewPaymentInstrumentResponse, error) {
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "preview payment instrument", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	normalizeSellerRequest := &sellersv1.NormalizeSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: req.GetPaymentInstrument().GetHolder().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetPaymentInstrument().GetAudit().GetCreatedBy(),
				CreatedAt: req.GetPaymentInstrument().GetAudit().GetCreatedAt(),
			},
		},
	}
	normalizeSellerReply, err := grpcx.Call(ctx, s.sellerValidationClient.NormalizeSeller, normalizeSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerValidationService/NormalizeSeller")
	}
	explainPayoutCalculationRequest := &paymentsv1.ExplainPayoutCalculationRequest{
		Context: req.GetContext(),
		Payout: &paymentsv1.Payout{
			Settlement: &paymentsv1.Payout_Bank{Bank: &paymentsv1.PayoutBankSettlement{Iban: req.GetPaymentInstrument().GetBank().GetIban()}},
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: req.GetPaymentInstrument().GetHolder().GetDisplayName(),
				Number:      normalizeSellerReply.GetSeller().GetNationalId().GetNumber(),
			},
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: normalizeSellerReply.GetSeller().GetPrimaryAddress().GetPostalCode(),
			},
		},
	}
	explainPayoutCalculationReply, err := paymentsv1.PayoutCalculationServiceClient.ExplainPayoutCalculation(s.payoutCalculationClient, ctx, explainPayoutCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutCalculationService/ExplainPayoutCalculation")
	}
	return &paymentsv1.PreviewPaymentInstrumentResponse{
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: normalizeSellerReply.GetSeller().GetAudit().GetUpdatedAt(),
			},
			Contact: &commonv1.ContactInfo{
				Email: normalizeSellerReply.GetSeller().GetRelationshipManager().GetEmail(),
			},
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			OpenedAt:            req.GetPaymentInstrument().GetOpenedAt(),
		},
		Result: &commonv1.OperationResult{
			Status: explainPayoutCalculationReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// CheckPaymentInstrumentEligibility checks a payment instrument.
func (s *PaymentInstrumentValidationServer) CheckPaymentInstrumentEligibility(ctx context.Context, req *paymentsv1.CheckPaymentInstrumentEligibilityRequest) (*paymentsv1.CheckPaymentInstrumentEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CheckPaymentInstrumentEligibility", "error", err)
		return nil, err
	}
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "check payment instrument eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	labels := make(map[string]string, len(req.GetPaymentInstrument().GetLabels()))
	for key, value := range req.GetPaymentInstrument().GetLabels() {
		labels[key] = value
	}
	findFraudSignalsReply, err := s.fraudSignalQueryClient.FindFraudSignals(ctx, &fraudv1.FindFraudSignalsRequest{
		Context: req.GetContext(),
		Filter:  &fraudv1.FindFraudSignalsRequest_Filter{Labels: labels},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalQueryService/FindFraudSignals")
	}
	paymentValidation, err := registry.Lookup[paymentsv1.PaymentValidationServiceClient](s.clients, "acme.payments.v1.PaymentValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	checkPaymentEligibilityRequest := &paymentsv1.CheckPaymentEligibilityRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.CheckPaymentEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetPaymentInstrument().GetContact().GetPreferredChannel(),
			},
		},
	}
	checkPaymentEligibilityReply, err := paymentValidation.CheckPaymentEligibility(ctx, checkPaymentEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentValidationService/CheckPaymentEligibility")
	}
	return &paymentsv1.CheckPaymentInstrumentEligibilityResponse{
		Outcome: &paymentsv1.CheckPaymentInstrumentEligibilityResponse_Outcome{Message: findFraudSignalsReply.GetResult().GetMessage()},
		Result: &commonv1.OperationResult{
			Details: findFraudSignalsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Audit: &commonv1.AuditInfo{
				CreatedBy: checkPaymentEligibilityReply.GetPayment().GetAudit().GetCreatedBy(),
			},
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			OpenedAt:            req.GetPaymentInstrument().GetOpenedAt(),
		},
	}, nil
}

// NormalizePaymentInstrument normalizes a payment instrument.
func (s *PaymentInstrumentValidationServer) NormalizePaymentInstrument(ctx context.Context, req *paymentsv1.NormalizePaymentInstrumentRequest) (*paymentsv1.NormalizePaymentInstrumentResponse, error) {
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "normalize payment instrument", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	source := req.GetPaymentInstrument().ProtoReflect()
	externalAccountId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("external_account_id"))).String()
	findShipmentsRequest := &shippingv1.FindShipmentsRequest{
		Context: req.GetContext(),
		Filter: &shippingv1.FindShipmentsRequest_Filter{
			Labels:       req.GetPaymentInstrument().GetLabels(),
			CreatedAfter: req.GetPaymentInstrument().GetAudit().GetUpdatedAt(),
			OwnerId:      externalAccountId,
		},
	}
	findShipmentsReply, err := shippingv1.ShipmentQueryServiceClient.FindShipments(s.shipmentQueryClient, ctx, findShipmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentQueryService/FindShipments")
	}
	inspectTenantRequest := &identityv1.InspectTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
	}
	inspectTenantReply, err := grpcx.Call(ctx, s.tenantAdminClient.InspectTenant, inspectTenantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/InspectTenant")
	}
	labels := make(map[string]string, len(req.GetPaymentInstrument().GetLabels()))
	for key, value := range req.GetPaymentInstrument().GetLabels() {
		labels[key] = value
	}
	countGiftCardBatchesReply, err := s.giftCardBatchQueryClient.CountGiftCardBatches(ctx, &giftcardsv1.CountGiftCardBatchesRequest{
		Context: req.GetContext(),
		Filter: &giftcardsv1.CountGiftCardBatchesRequest_Filter{
			Labels: labels,
			Region: inspectTenantReply.GetTenant().GetLocation().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchQueryService/CountGiftCardBatches")
	}
	cloned := proto.Clone(req.GetPaymentInstrument()).(*paymentsv1.PaymentInstrument)
	packed, err := anypb.New(req.GetPaymentInstrument())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	normalizeGiftCardTransactionRequest := &giftcardsv1.NormalizeGiftCardTransactionRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: inspectTenantReply.GetTenant().GetManager().GetDisplayName(),
			},
			Detail: packed,
		},
		Details: &giftcardsv1.NormalizeGiftCardTransactionRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: cloned.GetContact().GetPreferredChannel(),
				Name:             req.GetPaymentInstrument().GetContact().GetName(),
			},
		},
	}
	normalizeGiftCardTransactionReply, err := s.giftCardTransactionValidationLazy().NormalizeGiftCardTransaction(ctx, normalizeGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionValidationService/NormalizeGiftCardTransaction")
	}
	_ = countGiftCardBatchesReply
	return &paymentsv1.NormalizePaymentInstrumentResponse{
		Result: &commonv1.OperationResult{
			Details:      findShipmentsReply.GetResult().GetDetails(),
			ReferenceIds: findShipmentsReply.GetResult().GetReferenceIds(),
			Message:      normalizeGiftCardTransactionReply.GetOutcome().GetMessage(),
			Status:       "ok",
		},
		Outcome: &paymentsv1.NormalizePaymentInstrumentResponse_Outcome{
			Status:  inspectTenantReply.GetResult().GetStatus(),
			Message: normalizeGiftCardTransactionReply.GetResult().GetMessage(),
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			Balance:             req.GetPaymentInstrument().GetBalance(),
		},
	}, nil
}

// ExplainPaymentInstrumentDecision explains a payment instrument.
func (s *PaymentInstrumentValidationServer) ExplainPaymentInstrumentDecision(ctx context.Context, req *paymentsv1.ExplainPaymentInstrumentDecisionRequest) (*paymentsv1.ExplainPaymentInstrumentDecisionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "explain payment instrument decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	exportAccessPoliciesStream, err := s.accessPolicyQueryClient.ExportAccessPolicies(ctx, &authorizationv1.ExportAccessPoliciesRequest{
		Context: req.GetContext(),
		Filter: &authorizationv1.ExportAccessPoliciesRequest_Filter{
			Labels:       req.GetPaymentInstrument().GetLabels(),
			CreatedAfter: req.GetPaymentInstrument().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetPaymentInstrument().GetExternalAccountId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyQueryService/ExportAccessPolicies")
	}
	for {
		received, err := exportAccessPoliciesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyQueryService/ExportAccessPolicies")
		}
		_ = received
	}
	listMetricSeriesesRequest := &metricsv1.ListMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter: &metricsv1.ListMetricSeriesesRequest_Filter{
			Labels:       req.GetPaymentInstrument().GetLabels(),
			CreatedAfter: req.GetPaymentInstrument().GetAudit().GetUpdatedAt(),
		},
	}
	listMetricSeriesesReply, err := s.metricSeriesPort.ListMetricSerieses(ctx, listMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesService/ListMetricSerieses")
	}
	estimatePaymentInstrumentRequest := &paymentsv1.EstimatePaymentInstrumentRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetPaymentInstrument().GetAudit().GetCreatedAt(),
				Etag:      req.GetPaymentInstrument().GetAudit().GetEtag(),
			},
			Contact: &commonv1.ContactInfo{Email: req.GetPaymentInstrument().GetContact().GetEmail()},
		},
	}
	estimatePaymentInstrumentReply, err := grpcx.Call(ctx, s.paymentInstrumentCalculationClient.EstimatePaymentInstrument, estimatePaymentInstrumentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentCalculationService/EstimatePaymentInstrument")
	}
	return &paymentsv1.ExplainPaymentInstrumentDecisionResponse{
		Result: &commonv1.OperationResult{
			Details:       listMetricSeriesesReply.GetResult().GetDetails(),
			CorrelationId: listMetricSeriesesReply.GetResult().GetCorrelationId(),
			Message:       estimatePaymentInstrumentReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			Available:           req.GetPaymentInstrument().GetAvailable(),
		},
	}, nil
}

// ScorePaymentInstrument scores a payment instrument.
func (s *PaymentInstrumentValidationServer) ScorePaymentInstrument(ctx context.Context, req *paymentsv1.ScorePaymentInstrumentRequest) (*paymentsv1.ScorePaymentInstrumentResponse, error) {
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "score payment instrument", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	var accountHolder string
	switch choice := req.GetPaymentInstrument().GetFunding().(type) {
	case *paymentsv1.PaymentInstrument_Card:
		accountHolder = choice.Card.GetHolderName()
	case *paymentsv1.PaymentInstrument_Bank:
		accountHolder = choice.Bank.GetAccountHolder()
	case *paymentsv1.PaymentInstrument_ExternalAccountId:
		accountHolder = choice.ExternalAccountId
	}
	explainPaymentInstrumentCalculationReply, err := s.paymentInstrumentCalculationClient.ExplainPaymentInstrumentCalculation(ctx, &paymentsv1.ExplainPaymentInstrumentCalculationRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Funding: &paymentsv1.PaymentInstrument_Bank{Bank: &paymentsv1.PaymentInstrumentBankFunding{AccountHolder: accountHolder}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentCalculationService/ExplainPaymentInstrumentCalculation")
	}
	paymentInstrumentCache := cache.New[string, *paymentsv1.PaymentInstrument](1)
	paymentInstrumentCache.Put(req.GetContext().GetTraceId(), req.GetPaymentInstrument())
	cachedPaymentInstrument, _ := paymentInstrumentCache.Get(req.GetContext().GetTraceId())
	normalizeSearchQueryRequest := &searchv1.NormalizeSearchQueryRequest{
		Context: req.GetContext(),
		SearchQuery: &searchv1.SearchQuery{
			Subject: &searchv1.SearchIndexRef{
				DisplayName: cachedPaymentInstrument.GetHolder().GetDisplayName(),
			},
		},
		Details: &searchv1.NormalizeSearchQueryRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: explainPaymentInstrumentCalculationReply.GetPaymentInstrument().GetContact().GetPreferredChannel(),
				Email:            req.GetPaymentInstrument().GetHolder().GetEmail(),
			},
		},
	}
	normalizeSearchQueryReply, err := s.searchQueryValidationLazy().NormalizeSearchQuery(ctx, normalizeSearchQueryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryValidationService/NormalizeSearchQuery")
	}
	return &paymentsv1.ScorePaymentInstrumentResponse{
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Contact:             explainPaymentInstrumentCalculationReply.GetPaymentInstrument().GetContact(),
			PaymentInstrumentId: explainPaymentInstrumentCalculationReply.GetPaymentInstrument().GetPaymentInstrumentId(),
			Balance:             req.GetPaymentInstrument().GetBalance(),
		},
		Result: &commonv1.OperationResult{
			Message: normalizeSearchQueryReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Breakdown: &paymentsv1.ScorePaymentInstrumentResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
	}, nil
}

// LintPaymentInstruments lints a payment instrument.
func (s *PaymentInstrumentValidationServer) LintPaymentInstruments(ctx context.Context, req *paymentsv1.LintPaymentInstrumentsRequest) (*paymentsv1.LintPaymentInstrumentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LintPaymentInstruments", "error", err)
		return nil, err
	}
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "lint payment instruments", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	cloneDriverRequest := &driversv1.CloneDriverRequest{
		Context: req.GetContext(),
		Driver: &driversv1.Driver{
			RelationshipManager: &identityv1.UserRef{Email: req.GetDetails().GetContact().GetEmail()},
		},
	}
	cloneDriverReply, err := cloneDriverCall(ctx, cloneDriverRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.drivers.v1.DriverService/CloneDriver")
	}
	browseDemandForecastsReply, err := s.demandForecastQueryClient.BrowseDemandForecasts(ctx, &forecastingv1.BrowseDemandForecastsRequest{
		Context: req.GetContext(),
		Filter: &forecastingv1.BrowseDemandForecastsRequest_Filter{
			Region: cloneDriverReply.GetDriver().GetPrimaryAddress().GetRegion(),
			Labels: req.GetPaymentInstrument().GetLabels(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.forecasting.v1.DemandForecastQueryService/BrowseDemandForecasts")
	}
	giftCardTransactionValidation, ok := s.clients.Get("acme.giftcards.v1.GiftCardTransactionValidationService").(giftcardsv1.GiftCardTransactionValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.giftcards.v1.GiftCardTransactionValidationService")
	}
	encoded, err := protojson.Marshal(req.GetPaymentInstrument())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded paymentsv1.PaymentInstrument
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	checkGiftCardTransactionEligibilityRequest := &giftcardsv1.CheckGiftCardTransactionEligibilityRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.CheckGiftCardTransactionEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Name: (&decoded).GetContact().GetName()},
		},
	}
	checkGiftCardTransactionEligibilityReply, err := giftCardTransactionValidation.CheckGiftCardTransactionEligibility(ctx, checkGiftCardTransactionEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionValidationService/CheckGiftCardTransactionEligibility")
	}
	return &paymentsv1.LintPaymentInstrumentsResponse{
		Result:  &commonv1.OperationResult{Status: cloneDriverReply.GetResult().GetStatus()},
		Outcome: &paymentsv1.LintPaymentInstrumentsResponse_Outcome{Status: browseDemandForecastsReply.GetResult().GetStatus()},
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Holder: &customersv1.CustomerRef{
				DisplayName: checkGiftCardTransactionEligibilityReply.GetGiftCardTransaction().GetSubject().GetDisplayName(),
			},
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			Labels:              req.GetPaymentInstrument().GetLabels(),
		},
	}, nil
}

// giftCardTransactionValidationLazy dials acme.giftcards.v1.GiftCardTransactionValidationService the first time a
// handler needs it.
func (s *PaymentInstrumentValidationServer) giftCardTransactionValidationLazy() giftcardsv1.GiftCardTransactionValidationServiceClient {
	s.giftCardTransactionValidationOnce.Do(func() {
		s.giftCardTransactionValidationCached = giftcardsv1.NewGiftCardTransactionValidationServiceClient(s.conn)
	})
	return s.giftCardTransactionValidationCached
}

// searchQueryValidationLazy dials acme.search.v1.SearchQueryValidationService the first time a handler needs it.
func (s *PaymentInstrumentValidationServer) searchQueryValidationLazy() searchv1.SearchQueryValidationServiceClient {
	s.searchQueryValidationOnce.Do(func() { s.searchQueryValidationCached = searchv1.NewSearchQueryValidationServiceClient(s.conn) })
	return s.searchQueryValidationCached
}
