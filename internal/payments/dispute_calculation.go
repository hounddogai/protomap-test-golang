package payments

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// DisputeCalculationServer implements `acme.payments.v1.DisputeCalculationService`.
// Computes the figures of a dispute, such as totals, taxes, and estimates.
type DisputeCalculationServer struct {
	*disputeCalculationHandlers
}

// disputeCalculationHandlers holds the handlers of DisputeCalculationServer, with what they use.
type disputeCalculationHandlers struct {
	paymentsv1.UnimplementedDisputeCalculationServiceServer
	store                             *Store
	log                               *slog.Logger
	shipmentQueryClient               shippingv1.ShipmentQueryServiceClient
	conn                              *grpc.ClientConn
	productReviewQueryOnce            sync.Once
	productReviewQueryCached          reviewsv1.ProductReviewQueryServiceClient
	payoutCalculationClient           paymentsv1.PayoutCalculationServiceClient
	refundCalculationClient           paymentsv1.RefundCalculationServiceClient
	refundValidationClient            paymentsv1.RefundValidationServiceClient
	sellerListingQueryClient          sellersv1.SellerListingQueryServiceClient
	paymentInstrumentValidationClient paymentsv1.PaymentInstrumentValidationServiceClient
	featureFlagValidationClient       featureflagsv1.FeatureFlagValidationServiceClient
	meteredPaymentValidation          meteredPaymentValidation
	ledgerAccountClient               ledgerv1.LedgerAccountServiceClient
	metricSeriesQueryClient           metricsv1.MetricSeriesQueryServiceClient
	paymentInstrumentValidationPort   paymentInstrumentValidationPort
}

// NewDisputeCalculationServer wires a DisputeCalculationServer.
func NewDisputeCalculationServer(deps platform.Deps) *DisputeCalculationServer {
	return &DisputeCalculationServer{disputeCalculationHandlers: &disputeCalculationHandlers{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.payments.v1.DisputeCalculationService"),
		shipmentQueryClient:               shippingv1.NewShipmentQueryServiceClient(deps.Conn),
		conn:                              deps.Conn,
		payoutCalculationClient:           paymentsv1.NewPayoutCalculationServiceClient(deps.Conn),
		refundCalculationClient:           paymentsv1.NewRefundCalculationServiceClient(deps.Conn),
		refundValidationClient:            paymentsv1.NewRefundValidationServiceClient(deps.Conn),
		sellerListingQueryClient:          sellersv1.NewSellerListingQueryServiceClient(deps.Conn),
		paymentInstrumentValidationClient: paymentsv1.NewPaymentInstrumentValidationServiceClient(deps.Conn),
		featureFlagValidationClient:       featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn),
		meteredPaymentValidation:          meteredPaymentValidation{PaymentValidationServiceClient: paymentsv1.NewPaymentValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		ledgerAccountClient:               ledgerv1.NewLedgerAccountServiceClient(deps.Conn),
		metricSeriesQueryClient:           metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn),
		paymentInstrumentValidationPort:   newPaymentInstrumentValidationPort(deps),
	}}
}

// CalculateDispute calculates a dispute.
func (s *disputeCalculationHandlers) CalculateDispute(ctx context.Context, req *paymentsv1.CalculateDisputeRequest) (*paymentsv1.CalculateDisputeResponse, error) {
	if err := s.store.SaveDispute(ctx, req.GetContext().GetTenantId(), req.GetDispute()); err != nil {
		return nil, grpcx.Wrap(err, "save dispute")
	}
	s.log.InfoContext(ctx, "calculate dispute", "tenant", req.GetContext().GetTenantId(), "etag", req.GetDispute().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetDispute())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded paymentsv1.Dispute
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	browseShipmentsRequest := newBrowseShipmentsRequest(withBrowseShipmentsContext(req.GetContext()), withBrowseShipmentsFilter(&shippingv1.BrowseShipmentsRequest_Filter{
		Region:       req.GetDetails().GetDestination().GetRegion(),
		CreatedAfter: (&decoded).GetSla().GetDueAt(),
	}))
	browseShipmentsReply, err := s.shipmentQueryClient.BrowseShipments(ctx, browseShipmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentQueryService/BrowseShipments")
	}
	encoded2, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded2 paymentsv1.CalculateDisputeRequest_Details
	if err := protojson.Unmarshal(encoded2, &decoded2); err != nil {
		return nil, err
	}
	customFields := make(map[string]string, len(req.GetDispute().GetCustomFields()))
	for key, value := range req.GetDispute().GetCustomFields() {
		customFields[key] = value
	}
	browseProductReviewsRequest := &reviewsv1.BrowseProductReviewsRequest{
		Context: req.GetContext(),
		Filter: &reviewsv1.BrowseProductReviewsRequest_Filter{
			Region:       (&decoded2).GetDestination().GetRegion(),
			CreatedAfter: req.GetDispute().GetSla().GetDueAt(),
			OwnerId:      req.GetDispute().GetSla().GetPolicyId(),
			Labels:       customFields,
		},
	}
	browseProductReviewsReply, err := s.productReviewQueryLazy().BrowseProductReviews(ctx, browseProductReviewsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewQueryService/BrowseProductReviews")
	}
	return &paymentsv1.CalculateDisputeResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: browseShipmentsReply.GetResult().GetCorrelationId(),
			Status:        browseProductReviewsReply.GetResult().GetStatus(),
		},
		Dispute: &paymentsv1.Dispute{
			DisputeId: req.GetDisputeId(),
			Requester: req.GetDispute().GetRequester(),
		},
	}, nil
}

// EstimateDispute estimates a dispute.
func (s *disputeCalculationHandlers) EstimateDispute(ctx context.Context, req *paymentsv1.EstimateDisputeRequest) (*paymentsv1.EstimateDisputeResponse, error) {
	if err := s.store.SaveDispute(ctx, req.GetContext().GetTenantId(), req.GetDispute()); err != nil {
		return nil, grpcx.Wrap(err, "save dispute")
	}
	s.log.InfoContext(ctx, "estimate dispute", "tenant", req.GetContext().GetTenantId(), "etag", req.GetDispute().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*paymentsv1.EstimateDisputeRequest_Details)
	simulatePayoutRequest := &paymentsv1.SimulatePayoutRequest{
		Context: req.GetContext(),
		Payout: &paymentsv1.Payout{
			BillingAddress: &commonv1.PostalAddress{
				Locality:    req.GetDetails().GetDestination().GetLocality(),
				CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
			},
		},
		Details: &paymentsv1.SimulatePayoutRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
				Recipient:  cloned.GetDestination().GetRecipient(),
			},
		},
	}
	simulatePayoutReply, err := paymentsv1.PayoutCalculationServiceClient.SimulatePayout(s.payoutCalculationClient, ctx, simulatePayoutRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutCalculationService/SimulatePayout")
	}
	quoteRefundRequest := newQuoteRefundRequest(withQuoteRefundContext(req.GetContext()), withQuoteRefundDetails(&paymentsv1.QuoteRefundRequest_Details{
		Destination: &commonv1.PostalAddress{
			PostalCode: simulatePayoutReply.GetPayout().GetBillingAddress().GetPostalCode(),
			Locality:   simulatePayoutReply.GetPayout().GetBillingAddress().GetLocality(),
		},
	}), withQuoteRefundRefund(&paymentsv1.Refund{
		BillingAddress: &commonv1.PostalAddress{
			Recipient: simulatePayoutReply.GetPayout().GetBillingAddress().GetRecipient(),
		},
	}))
	quoteRefundReply, err := s.refundCalculationClient.QuoteRefund(ctx, quoteRefundRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundCalculationService/QuoteRefund")
	}
	checkRefundEligibilityReply, err := s.refundValidationClient.CheckRefundEligibility(ctx, &paymentsv1.CheckRefundEligibilityRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.CheckRefundEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDispute().GetAssignee().GetEmail()},
		},
		Refund: &paymentsv1.Refund{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetDispute().GetAudit().GetCreatedBy()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundValidationService/CheckRefundEligibility")
	}
	return &paymentsv1.EstimateDisputeResponse{
		Breakdown: &paymentsv1.EstimateDisputeResponse_Breakdown{
			CurrencyCode: simulatePayoutReply.GetBreakdown().GetCurrencyCode(),
		},
		Dispute: &paymentsv1.Dispute{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: simulatePayoutReply.GetPayout().GetAudit().GetUpdatedAt(),
				Etag:      quoteRefundReply.GetRefund().GetAudit().GetEtag(),
				CreatedBy: checkRefundEligibilityReply.GetRefund().GetAudit().GetCreatedBy(),
			},
			DisputeId:    req.GetDisputeId(),
			CustomFields: req.GetDispute().GetCustomFields(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// QuoteDispute quotes a dispute.
func (s *disputeCalculationHandlers) QuoteDispute(ctx context.Context, req *paymentsv1.QuoteDisputeRequest) (*paymentsv1.QuoteDisputeResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected QuoteDispute", "error", err)
		return nil, err
	}
	if err := s.store.SaveDispute(ctx, req.GetContext().GetTenantId(), req.GetDispute()); err != nil {
		return nil, grpcx.Wrap(err, "save dispute")
	}
	s.log.InfoContext(ctx, "quote dispute", "tenant", req.GetContext().GetTenantId(), "etag", req.GetDispute().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeSellerListingsRequest := &sellersv1.SummarizeSellerListingsRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: annotations,
		Details:     &sellersv1.SummarizeSellerListingsRequest_Details{Filters: req.GetDispute().GetCustomFields()},
	}
	summarizeSellerListingsReply, err := sellersv1.SellerListingQueryServiceClient.SummarizeSellerListings(s.sellerListingQueryClient, ctx, summarizeSellerListingsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingQueryService/SummarizeSellerListings")
	}
	scoreRefundRequest := &paymentsv1.ScoreRefundRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.ScoreRefundRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
				Locality:   req.GetDetails().GetDestination().GetLocality(),
			},
		},
		Refund: &paymentsv1.Refund{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
			},
		},
	}
	type scoreRefundResult struct {
		reply *paymentsv1.ScoreRefundResponse
		err   error
	}
	scoreRefundResults := make(chan scoreRefundResult, 1)
	go func() {
		reply, err := s.refundValidationClient.ScoreRefund(ctx, scoreRefundRequest)
		scoreRefundResults <- scoreRefundResult{reply, err}
	}()
	var scoreRefundReply *paymentsv1.ScoreRefundResponse
	select {
	case result := <-scoreRefundResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.payments.v1.RefundValidationService/ScoreRefund")
		}
		scoreRefundReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &paymentsv1.QuoteDisputeResponse{
		Result: &commonv1.OperationResult{
			Details:      summarizeSellerListingsReply.GetResult().GetDetails(),
			ReferenceIds: summarizeSellerListingsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Dispute: &paymentsv1.Dispute{
			Sla:       &paymentsv1.DisputeSla{DueAt: scoreRefundReply.GetRefund().GetDueAt()},
			DisputeId: req.GetDisputeId(),
			Assignee:  req.GetDispute().GetAssignee(),
		},
	}, nil
}

// CompareDisputeOptions compares a dispute.
func (s *disputeCalculationHandlers) CompareDisputeOptions(ctx context.Context, req *paymentsv1.CompareDisputeOptionsRequest) (*paymentsv1.CompareDisputeOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveDispute(ctx, req.GetContext().GetTenantId(), req.GetDispute()); err != nil {
		return nil, grpcx.Wrap(err, "save dispute")
	}
	s.log.InfoContext(ctx, "compare dispute options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetDispute().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded paymentsv1.CompareDisputeOptionsRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	explainParcelCalculationRequest := &shippingv1.ExplainParcelCalculationRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetDispute().GetCustomer().GetDisplayName(),
			},
			Locator: &shippingv1.Parcel_Address{Address: &commonv1.PostalAddress{CountryCode: (&decoded).GetDestination().GetCountryCode()}},
			Audit:   &commonv1.AuditInfo{UpdatedAt: req.GetDispute().GetAudit().GetUpdatedAt()},
		},
		Details: &shippingv1.ExplainParcelCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetDispute().GetRequester().GetPhone()},
		},
	}
	explainParcelCalculationReply := new(shippingv1.ExplainParcelCalculationResponse)
	if err := grpcx.Invoke(ctx, s.conn, shippingv1.ParcelCalculationService_ExplainParcelCalculation_FullMethodName, explainParcelCalculationRequest, explainParcelCalculationReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/ExplainParcelCalculation")
	}
	explainRefundCalculationRequest := &paymentsv1.ExplainRefundCalculationRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.ExplainRefundCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDispute().GetRequester().GetEmail()},
		},
		Refund: &paymentsv1.Refund{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: explainParcelCalculationReply.GetParcel().GetAudit().GetUpdatedBy(),
			},
			Counterparty: &customersv1.CustomerRef{
				DisplayName: req.GetDispute().GetCustomer().GetDisplayName(),
			},
		},
	}
	type explainRefundCalculationResult struct {
		reply *paymentsv1.ExplainRefundCalculationResponse
		err   error
	}
	explainRefundCalculationResults := make(chan explainRefundCalculationResult, 1)
	go func() {
		reply, err := s.refundCalculationClient.ExplainRefundCalculation(ctx, explainRefundCalculationRequest)
		explainRefundCalculationResults <- explainRefundCalculationResult{reply, err}
	}()
	var explainRefundCalculationReply *paymentsv1.ExplainRefundCalculationResponse
	select {
	case result := <-explainRefundCalculationResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.payments.v1.RefundCalculationService/ExplainRefundCalculation")
		}
		explainRefundCalculationReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	step := &disputeCalculationHandlersCompareDisputeOptionsNormalizePaymentInstrumentStep{request: &paymentsv1.NormalizePaymentInstrumentRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.NormalizePaymentInstrumentRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDispute().GetAssignee().GetEmail()},
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Funding: &paymentsv1.PaymentInstrument_Card{Card: &paymentsv1.PaymentInstrumentCardFunding{
				Token: explainRefundCalculationReply.GetRefund().GetCard().GetToken(),
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: explainParcelCalculationReply.GetParcel().GetAudit().GetUpdatedBy(),
			},
		},
	}}
	if err := s.runCompareDisputeOptionsNormalizePaymentInstrument(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentValidationService/NormalizePaymentInstrument")
	}
	normalizePaymentInstrumentReply := step.reply
	return &paymentsv1.CompareDisputeOptionsResponse{
		Result: &commonv1.OperationResult{
			Message: explainParcelCalculationReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Breakdown: &paymentsv1.CompareDisputeOptionsResponse_Breakdown{Total: explainRefundCalculationReply.GetRefund().GetTotal()},
		Dispute: &paymentsv1.Dispute{
			Requester: &commonv1.ContactInfo{
				PreferredChannel: normalizePaymentInstrumentReply.GetPaymentInstrument().GetContact().GetPreferredChannel(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: normalizePaymentInstrumentReply.GetPaymentInstrument().GetAudit().GetCreatedAt(),
			},
			DisputeId: req.GetDisputeId(),
			Subject:   req.GetDispute().GetSubject(),
		},
	}, nil
}

// RecalculateDisputes recalculates a dispute.
func (s *disputeCalculationHandlers) RecalculateDisputes(ctx context.Context, req *paymentsv1.RecalculateDisputesRequest) (*paymentsv1.RecalculateDisputesResponse, error) {
	if err := s.store.SaveDispute(ctx, req.GetContext().GetTenantId(), req.GetDispute()); err != nil {
		return nil, grpcx.Wrap(err, "save dispute")
	}
	s.log.InfoContext(ctx, "recalculate disputes", "tenant", req.GetContext().GetTenantId(), "etag", req.GetDispute().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*paymentsv1.RecalculatePayoutsResponse, len(req.GetDispute().GetComments()))
	for i, comment := range req.GetDispute().GetComments() {
		group.Go(func() error {
			reply, err := s.payoutCalculationClient.RecalculatePayouts(groupCtx, &paymentsv1.RecalculatePayoutsRequest{
				Context: req.GetContext(),
				Payout: &paymentsv1.Payout{
					Audit: &commonv1.AuditInfo{CreatedAt: comment.GetCreatedAt()},
				},
				Details: &paymentsv1.RecalculatePayoutsRequest_Details{
					Destination: &commonv1.PostalAddress{Locality: req.GetDetails().GetDestination().GetLocality()},
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
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutCalculationService/RecalculatePayouts")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := disputeCalculationHandlersCheckPaymentInstrumentEligibilitySteps[mode]
	checkPaymentInstrumentEligibilityRequest := &paymentsv1.CheckPaymentInstrumentEligibilityRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.CheckPaymentInstrumentEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDispute().GetRequester().GetPreferredChannel(),
				Name:             req.GetDispute().GetRequester().GetName(),
			},
		},
	}
	checkPaymentInstrumentEligibilityReply, err := run(s, ctx, checkPaymentInstrumentEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentValidationService/CheckPaymentInstrumentEligibility")
	}
	cloned := proto.Clone(req.GetDispute()).(*paymentsv1.Dispute)
	previewFeatureFlagRequest := &featureflagsv1.PreviewFeatureFlagRequest{
		Context: req.GetContext(),
		Details: &featureflagsv1.PreviewFeatureFlagRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: checkPaymentInstrumentEligibilityReply.GetPaymentInstrument().GetContact().GetPreferredChannel(),
				Phone:            cloned.GetRequester().GetPhone(),
			},
		},
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: checkPaymentInstrumentEligibilityReply.GetPaymentInstrument().GetAudit().GetUpdatedBy(),
			},
		},
	}
	previewFeatureFlagReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*featureflagsv1.PreviewFeatureFlagResponse, error) {
		return s.featureFlagValidationClient.PreviewFeatureFlag(ctx, previewFeatureFlagRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/PreviewFeatureFlag")
	}
	return &paymentsv1.RecalculateDisputesResponse{
		Dispute: &paymentsv1.Dispute{
			Audit: &commonv1.AuditInfo{
				Etag: checkPaymentInstrumentEligibilityReply.GetPaymentInstrument().GetAudit().GetEtag(),
			},
			DisputeId: req.GetDisputeId(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: previewFeatureFlagReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Breakdown: &paymentsv1.RecalculateDisputesResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
	}, nil
}

// SimulateDispute simulates a dispute.
func (s *disputeCalculationHandlers) SimulateDispute(ctx context.Context, req *paymentsv1.SimulateDisputeRequest) (*paymentsv1.SimulateDisputeResponse, error) {
	if err := s.store.SaveDispute(ctx, req.GetContext().GetTenantId(), req.GetDispute()); err != nil {
		return nil, grpcx.Wrap(err, "save dispute")
	}
	s.log.InfoContext(ctx, "simulate dispute", "tenant", req.GetContext().GetTenantId(), "etag", req.GetDispute().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetDispute())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded paymentsv1.Dispute
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	scorePaymentRequest := &paymentsv1.ScorePaymentRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			BillingAddress: &commonv1.PostalAddress{Recipient: req.GetDetails().GetDestination().GetRecipient()},
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetDispute().GetAudit().GetEtag(),
				UpdatedBy: (&decoded).GetAudit().GetUpdatedBy(),
			},
			Counterparty: &paymentsv1.RefundRef{Number: req.GetDispute().GetCustomer().GetNumber()},
		},
	}
	scorePaymentReply, err := s.meteredPaymentValidation.ScorePayment(ctx, scorePaymentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentValidationService/ScorePayment")
	}
	scoreRefundRequest := &paymentsv1.ScoreRefundRequest{
		Context: req.GetContext(),
		Refund: &paymentsv1.Refund{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: scorePaymentReply.GetPayment().GetCounterparty().GetDisplayName(),
			},
		},
	}
	type scoreRefundResult struct {
		reply *paymentsv1.ScoreRefundResponse
		err   error
	}
	scoreRefundResults := make(chan scoreRefundResult, 1)
	go func() {
		reply, err := s.refundValidationClient.ScoreRefund(ctx, scoreRefundRequest)
		scoreRefundResults <- scoreRefundResult{reply, err}
	}()
	var scoreRefundReply *paymentsv1.ScoreRefundResponse
	select {
	case result := <-scoreRefundResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.payments.v1.RefundValidationService/ScoreRefund")
		}
		scoreRefundReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	cloneLedgerAccountRequest := &ledgerv1.CloneLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Funding: &ledgerv1.LedgerAccount_Card{Card: &ledgerv1.LedgerAccountCardFunding{Last4: scorePaymentReply.GetPayment().GetCard().GetLast4()}},
			Contact: &commonv1.ContactInfo{Email: req.GetDispute().GetRequester().GetEmail()},
		},
	}
	cloneLedgerAccountReply, err := grpcx.Call(ctx, s.ledgerAccountClient.CloneLedgerAccount, cloneLedgerAccountRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/CloneLedgerAccount")
	}
	quoteRefundRequest := &paymentsv1.QuoteRefundRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.QuoteRefundRequest_Details{
			Destination: &commonv1.PostalAddress{
				Lines: scorePaymentReply.GetPayment().GetBillingAddress().GetLines(),
			},
		},
		Refund: &paymentsv1.Refund{
			BillingAddress: &commonv1.PostalAddress{
				Lines:      req.GetDetails().GetDestination().GetLines(),
				PostalCode: scorePaymentReply.GetPayment().GetBillingAddress().GetPostalCode(),
			},
			Settlement: &paymentsv1.Refund_Bank{Bank: &paymentsv1.RefundBankSettlement{Iban: scoreRefundReply.GetRefund().GetBank().GetIban()}},
		},
	}
	quoteRefundReply := new(paymentsv1.QuoteRefundResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", paymentsRefundCalculationServiceName, "QuoteRefund"), quoteRefundRequest, quoteRefundReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundCalculationService/QuoteRefund")
	}
	return &paymentsv1.SimulateDisputeResponse{
		Result: &commonv1.OperationResult{
			Details:       scorePaymentReply.GetResult().GetDetails(),
			CorrelationId: scorePaymentReply.GetResult().GetCorrelationId(),
			ReferenceIds:  cloneLedgerAccountReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		Dispute: &paymentsv1.Dispute{
			Audit: &commonv1.AuditInfo{
				CreatedAt: scoreRefundReply.GetRefund().GetAudit().GetCreatedAt(),
			},
			Assignee: &identityv1.UserRef{
				Email: cloneLedgerAccountReply.GetLedgerAccount().GetContact().GetEmail(),
			},
			DisputeId: req.GetDisputeId(),
			Subject:   req.GetDispute().GetSubject(),
		},
		Breakdown: &paymentsv1.SimulateDisputeResponse_Breakdown{
			CurrencyCode: quoteRefundReply.GetBreakdown().GetCurrencyCode(),
		},
	}, nil
}

// ExplainDisputeCalculation explains a dispute.
func (s *disputeCalculationHandlers) ExplainDisputeCalculation(ctx context.Context, req *paymentsv1.ExplainDisputeCalculationRequest) (*paymentsv1.ExplainDisputeCalculationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainDisputeCalculation", "error", err)
		return nil, err
	}
	if err := s.store.SaveDispute(ctx, req.GetContext().GetTenantId(), req.GetDispute()); err != nil {
		return nil, grpcx.Wrap(err, "save dispute")
	}
	s.log.InfoContext(ctx, "explain dispute calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetDispute().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "payment_id", req.GetDispute().GetCustomer().GetPaymentId())
	customFields := make(map[string]string, len(req.GetDispute().GetCustomFields()))
	for key, value := range req.GetDispute().GetCustomFields() {
		customFields[key] = value
	}
	suggestMetricSeriesesReply, err := s.metricSeriesQueryClient.SuggestMetricSerieses(ctx, &metricsv1.SuggestMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter: &metricsv1.SuggestMetricSeriesesRequest_Filter{
			CreatedAfter: req.GetDispute().GetAudit().GetUpdatedAt(),
			OwnerId:      tenancy.Value(ctx, "payment_id"),
			Labels:       customFields,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/SuggestMetricSerieses")
	}
	return &paymentsv1.ExplainDisputeCalculationResponse{
		Outcome: &paymentsv1.ExplainDisputeCalculationResponse_Outcome{
			Status:  suggestMetricSeriesesReply.GetResult().GetStatus(),
			Message: suggestMetricSeriesesReply.GetResult().GetMessage(),
		},
		Dispute: &paymentsv1.Dispute{
			DisputeId: req.GetDisputeId(),
			Subject:   req.GetDispute().GetSubject(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetDisputeRates returns a dispute.
func (s *disputeCalculationHandlers) GetDisputeRates(ctx context.Context, req *paymentsv1.GetDisputeRatesRequest) (*paymentsv1.GetDisputeRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "dispute", req.GetDisputeId(), "get_dispute_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get dispute rates", "tenant", req.GetContext().GetTenantId())
	validatePaymentInstrumentRequest := &paymentsv1.ValidatePaymentInstrumentRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		Details: &paymentsv1.ValidatePaymentInstrumentRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	validatePaymentInstrumentReply, err := s.paymentInstrumentValidationPort.ValidatePaymentInstrument(ctx, validatePaymentInstrumentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentValidationService/ValidatePaymentInstrument")
	}
	recalculatePayoutsRequest := &paymentsv1.RecalculatePayoutsRequest{
		Context: req.GetContext(),
		Payout: &paymentsv1.Payout{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: validatePaymentInstrumentReply.GetPaymentInstrument().GetAudit().GetUpdatedBy(),
			},
			Settlement: &paymentsv1.Payout_Bank{Bank: &paymentsv1.PayoutBankSettlement{
				Iban: validatePaymentInstrumentReply.GetPaymentInstrument().GetBank().GetIban(),
			}},
		},
	}
	send := s.recalculatePayoutsSender(recalculatePayoutsRequest)
	recalculatePayoutsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutCalculationService/RecalculatePayouts")
	}
	return &paymentsv1.GetDisputeRatesResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: validatePaymentInstrumentReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Dispute: &paymentsv1.Dispute{
			Assignee: &identityv1.UserRef{
				DisplayName: recalculatePayoutsReply.GetPayout().GetCounterparty().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: recalculatePayoutsReply.GetPayout().GetAudit().GetCreatedBy(),
			},
			DisputeId: req.GetDisputeId(),
		},
	}, nil
}

// productReviewQueryLazy dials acme.reviews.v1.ProductReviewQueryService the first time a handler needs it.
func (s *disputeCalculationHandlers) productReviewQueryLazy() reviewsv1.ProductReviewQueryServiceClient {
	s.productReviewQueryOnce.Do(func() { s.productReviewQueryCached = reviewsv1.NewProductReviewQueryServiceClient(s.conn) })
	return s.productReviewQueryCached
}

// disputeCalculationHandlersCompareDisputeOptionsNormalizePaymentInstrumentStep carries the request of one step of
// CompareDisputeOptions and, once it ran, its reply.
type disputeCalculationHandlersCompareDisputeOptionsNormalizePaymentInstrumentStep struct {
	request *paymentsv1.NormalizePaymentInstrumentRequest
	reply   *paymentsv1.NormalizePaymentInstrumentResponse
}

func (s *disputeCalculationHandlers) runCompareDisputeOptionsNormalizePaymentInstrument(ctx context.Context, step *disputeCalculationHandlersCompareDisputeOptionsNormalizePaymentInstrumentStep) error {
	reply, err := s.paymentInstrumentValidationClient.NormalizePaymentInstrument(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// disputeCalculationHandlersCheckPaymentInstrumentEligibilitySteps are the ways to run
// acme.payments.v1.PaymentInstrumentValidationService/CheckPaymentInstrumentEligibility, by name.
var disputeCalculationHandlersCheckPaymentInstrumentEligibilitySteps = map[string]func(*disputeCalculationHandlers, context.Context, *paymentsv1.CheckPaymentInstrumentEligibilityRequest) (*paymentsv1.CheckPaymentInstrumentEligibilityResponse, error){
	"default": (*disputeCalculationHandlers).checkPaymentInstrumentEligibilityStep,
	"audited": (*disputeCalculationHandlers).checkPaymentInstrumentEligibilityAuditedStep,
}

func (s *disputeCalculationHandlers) checkPaymentInstrumentEligibilityStep(ctx context.Context, request *paymentsv1.CheckPaymentInstrumentEligibilityRequest) (*paymentsv1.CheckPaymentInstrumentEligibilityResponse, error) {
	return s.paymentInstrumentValidationClient.CheckPaymentInstrumentEligibility(ctx, request)
}

func (s *disputeCalculationHandlers) checkPaymentInstrumentEligibilityAuditedStep(ctx context.Context, request *paymentsv1.CheckPaymentInstrumentEligibilityRequest) (*paymentsv1.CheckPaymentInstrumentEligibilityResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.payments.v1.PaymentInstrumentValidationService/CheckPaymentInstrumentEligibility")
	return s.paymentInstrumentValidationClient.CheckPaymentInstrumentEligibility(ctx, request)
}

// recalculatePayoutsSender returns a function that sends one request to
// acme.payments.v1.PayoutCalculationService/RecalculatePayouts.
func (s *disputeCalculationHandlers) recalculatePayoutsSender(request *paymentsv1.RecalculatePayoutsRequest) func(context.Context) (*paymentsv1.RecalculatePayoutsResponse, error) {
	return func(ctx context.Context) (*paymentsv1.RecalculatePayoutsResponse, error) {
		return s.payoutCalculationClient.RecalculatePayouts(ctx, request)
	}
}
