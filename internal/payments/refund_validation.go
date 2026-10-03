package payments

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// RefundValidationServer implements `acme.payments.v1.RefundValidationService`.
// Checks refunds against the rules before anything acts on them.
type RefundValidationServer struct {
	paymentsv1.UnimplementedRefundValidationServiceServer
	store                    *Store
	log                      *slog.Logger
	conn                     *grpc.ClientConn
	paymentValidationClient  paymentsv1.PaymentValidationServiceClient
	meteredFeatureFlag       meteredFeatureFlag
	paymentCalculationClient paymentsv1.PaymentCalculationServiceClient
	refundCalculationClient  paymentsv1.RefundCalculationServiceClient
	returnRequestClient      returnsv1.ReturnRequestServiceClient
	clients                  *registry.Registry
}

// NewRefundValidationServer wires a RefundValidationServer.
func NewRefundValidationServer(deps platform.Deps) *RefundValidationServer {
	return &RefundValidationServer{
		store:                    NewStore(deps.DB),
		log:                      deps.Logger.With("service", "acme.payments.v1.RefundValidationService"),
		conn:                     deps.Conn,
		paymentValidationClient:  paymentsv1.NewPaymentValidationServiceClient(deps.Conn),
		meteredFeatureFlag:       meteredFeatureFlag{FeatureFlagServiceClient: featureflagsv1.NewFeatureFlagServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		paymentCalculationClient: paymentsv1.NewPaymentCalculationServiceClient(deps.Conn),
		refundCalculationClient:  paymentsv1.NewRefundCalculationServiceClient(deps.Conn),
		returnRequestClient:      returnsv1.NewReturnRequestServiceClient(deps.Conn),
		clients:                  deps.Clients,
	}
}

// ValidateRefund validates a refund.
func (s *RefundValidationServer) ValidateRefund(ctx context.Context, req *paymentsv1.ValidateRefundRequest) (*paymentsv1.ValidateRefundResponse, error) {
	if err := s.store.SaveRefund(ctx, req.GetContext().GetTenantId(), req.GetRefund()); err != nil {
		return nil, grpcx.Wrap(err, "save refund")
	}
	s.log.InfoContext(ctx, "validate refund", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRefund().GetAudit().GetEtag())
	var holder struct{ Email string }
	if record := req.GetRefund().GetCounterparty(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	explainPointsLedgerDecisionRequest := &loyaltyv1.ExplainPointsLedgerDecisionRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Contact: &commonv1.ContactInfo{
				Email: holder.Email,
				Name:  req.GetDetails().GetContact().GetName(),
			},
			Funding: &loyaltyv1.PointsLedger_Bank{Bank: &loyaltyv1.PointsLedgerBankFunding{AccountHolder: req.GetRefund().GetBank().GetAccountHolder()}},
			Audit:   &commonv1.AuditInfo{UpdatedBy: req.GetRefund().GetAudit().GetUpdatedBy()},
		},
	}
	explainPointsLedgerDecisionReply := new(loyaltyv1.ExplainPointsLedgerDecisionResponse)
	if err := grpcx.Invoke(ctx, s.conn, loyaltyv1.PointsLedgerValidationService_ExplainPointsLedgerDecision_FullMethodName, explainPointsLedgerDecisionRequest, explainPointsLedgerDecisionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerValidationService/ExplainPointsLedgerDecision")
	}
	normalizePaymentRequest := &paymentsv1.NormalizePaymentRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.NormalizePaymentRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:  req.GetDetails().GetContact().GetName(),
				Phone: explainPointsLedgerDecisionReply.GetPointsLedger().GetContact().GetPhone(),
			},
		},
		Payment: &paymentsv1.Payment{
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: explainPointsLedgerDecisionReply.GetPointsLedger().GetHolder().GetDisplayName(),
			},
		},
	}
	normalizePaymentReply := new(paymentsv1.NormalizePaymentResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", paymentsPaymentValidationServiceName, "NormalizePayment"), normalizePaymentRequest, normalizePaymentReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentValidationService/NormalizePayment")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*paymentsv1.PreviewPaymentResponse, len(req.GetRefund().GetAttachments()))
	for i, attachment := range req.GetRefund().GetAttachments() {
		group.Go(func() error {
			reply, err := s.paymentValidationClient.PreviewPayment(groupCtx, &paymentsv1.PreviewPaymentRequest{
				Context: req.GetContext(),
				Payment: &paymentsv1.Payment{
					Attachments: []*paymentsv1.PaymentAttachment{&paymentsv1.PaymentAttachment{
						Uri:         attachment.GetUri(),
						FileName:    attachment.GetFileName(),
						ContentType: attachment.GetContentType(),
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
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentValidationService/PreviewPayment")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &paymentsv1.ValidateRefundResponse{
		Result: &commonv1.OperationResult{
			Details:       explainPointsLedgerDecisionReply.GetResult().GetDetails(),
			CorrelationId: explainPointsLedgerDecisionReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Refund: &paymentsv1.Refund{
			Number:   normalizePaymentReply.GetPayment().GetCounterparty().GetNumber(),
			RefundId: req.GetRefundId(),
			Status:   req.GetRefund().GetStatus(),
		},
	}, nil
}

// PreviewRefund previews a refund.
func (s *RefundValidationServer) PreviewRefund(ctx context.Context, req *paymentsv1.PreviewRefundRequest) (*paymentsv1.PreviewRefundResponse, error) {
	if err := s.store.SaveRefund(ctx, req.GetContext().GetTenantId(), req.GetRefund()); err != nil {
		return nil, grpcx.Wrap(err, "save refund")
	}
	s.log.InfoContext(ctx, "preview refund", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRefund().GetAudit().GetEtag())
	createFeatureFlagRequest := &featureflagsv1.CreateFeatureFlagRequest{
		Context: req.GetContext(),
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetRefund().GetAudit().GetCreatedBy()},
			Owner: &identityv1.UserRef{
				DisplayName: req.GetRefund().GetCounterparty().GetDisplayName(),
			},
		},
	}
	createFeatureFlagReply, err := s.meteredFeatureFlag.CreateFeatureFlag(ctx, createFeatureFlagRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagService/CreateFeatureFlag")
	}
	return &paymentsv1.PreviewRefundResponse{
		Refund: &paymentsv1.Refund{
			Audit: &commonv1.AuditInfo{
				CreatedBy: createFeatureFlagReply.GetFeatureFlag().GetAudit().GetCreatedBy(),
			},
			RefundId: req.GetRefundId(),
			IssuedAt: req.GetRefund().GetIssuedAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckRefundEligibility checks a refund.
func (s *RefundValidationServer) CheckRefundEligibility(ctx context.Context, req *paymentsv1.CheckRefundEligibilityRequest) (*paymentsv1.CheckRefundEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CheckRefundEligibility", "error", err)
		return nil, err
	}
	if err := s.store.SaveRefund(ctx, req.GetContext().GetTenantId(), req.GetRefund()); err != nil {
		return nil, grpcx.Wrap(err, "save refund")
	}
	s.log.InfoContext(ctx, "check refund eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRefund().GetAudit().GetEtag())
	refundCache := cache.New[string, *paymentsv1.Refund](1)
	refundCache.Put(req.GetContext().GetTraceId(), req.GetRefund())
	cachedRefund, _ := refundCache.Get(req.GetContext().GetTraceId())
	estimatePaymentRequest := &paymentsv1.EstimatePaymentRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Audit: &commonv1.AuditInfo{UpdatedAt: cachedRefund.GetAudit().GetUpdatedAt()},
		},
		Details: &paymentsv1.EstimatePaymentRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: req.GetRefund().GetBillingAddress().GetPostalCode(),
			},
		},
	}
	estimatePaymentReply, err := grpcx.Call(ctx, s.paymentCalculationClient.EstimatePayment, estimatePaymentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/EstimatePayment")
	}
	cloned := proto.Clone(req.GetRefund()).(*paymentsv1.Refund)
	compareRefundOptionsRequest := &paymentsv1.CompareRefundOptionsRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.CompareRefundOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{Recipient: cloned.GetBillingAddress().GetRecipient()},
		},
		Refund: &paymentsv1.Refund{
			Settlement: &paymentsv1.Refund_Card{Card: &paymentsv1.RefundCardSettlement{
				Token: estimatePaymentReply.GetPayment().GetCard().GetToken(),
			}},
		},
	}
	compareRefundOptionsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*paymentsv1.CompareRefundOptionsResponse, error) {
		return s.refundCalculationClient.CompareRefundOptions(ctx, compareRefundOptionsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundCalculationService/CompareRefundOptions")
	}
	previewSellerListingRequest := &sellersv1.PreviewSellerListingRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetRefund().GetAudit().GetCreatedAt()},
		},
	}
	previewSellerListingReply := new(sellersv1.PreviewSellerListingResponse)
	if err := grpc.Invoke(ctx, "/acme.sellers.v1.SellerListingValidationService/PreviewSellerListing", previewSellerListingRequest, previewSellerListingReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingValidationService/PreviewSellerListing")
	}
	quotePaymentRequest := buildQuotePaymentRequestForCheckRefundEligibility(req)
	quotePaymentReply, err := s.paymentCalculationClient.QuotePayment(ctx, quotePaymentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/QuotePayment")
	}
	_ = compareRefundOptionsReply
	return &paymentsv1.CheckRefundEligibilityResponse{
		Refund: &paymentsv1.Refund{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: estimatePaymentReply.GetPayment().GetAudit().GetUpdatedBy(),
				CreatedAt: quotePaymentReply.GetPayment().GetAudit().GetCreatedAt(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Locality:    estimatePaymentReply.GetPayment().GetBillingAddress().GetLocality(),
				CountryCode: previewSellerListingReply.GetSellerListing().GetAddress().GetCountryCode(),
			},
			Number:   quotePaymentReply.GetPayment().GetCounterparty().GetNumber(),
			RefundId: req.GetRefundId(),
			DueAt:    req.GetRefund().GetDueAt(),
		},
		Outcome: &paymentsv1.CheckRefundEligibilityResponse_Outcome{Message: previewSellerListingReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizeRefund normalizes a refund.
func (s *RefundValidationServer) NormalizeRefund(ctx context.Context, req *paymentsv1.NormalizeRefundRequest) (*paymentsv1.NormalizeRefundResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected NormalizeRefund", "error", err)
		return nil, err
	}
	if err := s.store.SaveRefund(ctx, req.GetContext().GetTenantId(), req.GetRefund()); err != nil {
		return nil, grpcx.Wrap(err, "save refund")
	}
	s.log.InfoContext(ctx, "normalize refund", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRefund().GetAudit().GetEtag())
	var holder struct{ PreferredChannel string }
	if record := req.GetDetails().GetContact(); record != nil {
		if err := mapper.CopyFields(&holder, record, "PreferredChannel"); err != nil {
			s.log.WarnContext(ctx, "copy preferred_channel", "error", err)
		}
	}
	watchReturnRequestsStream, err := s.returnRequestClient.WatchReturnRequests(ctx, &returnsv1.WatchReturnRequestsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			PreferredChannel: holder.PreferredChannel,
			Email:            req.GetRefund().GetCounterparty().GetEmail(),
			Phone:            req.GetDetails().GetContact().GetPhone(),
			Name:             req.GetDetails().GetContact().GetName(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestService/WatchReturnRequests")
	}
	for {
		received, err := watchReturnRequestsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestService/WatchReturnRequests")
		}
		_ = received
	}
	paymentCalculation, ok := s.clients.Get("acme.payments.v1.PaymentCalculationService").(paymentsv1.PaymentCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.payments.v1.PaymentCalculationService")
	}
	ctx = tenancy.With(ctx, "locality", req.GetRefund().GetBillingAddress().GetLocality())
	cloned := proto.Clone(req.GetRefund()).(*paymentsv1.Refund)
	estimatePaymentRequest := &paymentsv1.EstimatePaymentRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetRefund().GetAudit().GetUpdatedAt(),
				UpdatedBy: req.GetRefund().GetAudit().GetUpdatedBy(),
			},
		},
		Details: &paymentsv1.EstimatePaymentRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality:    tenancy.Value(ctx, "locality"),
				CountryCode: cloned.GetBillingAddress().GetCountryCode(),
			},
		},
	}
	estimatePaymentReply, err := paymentCalculation.EstimatePayment(ctx, estimatePaymentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/EstimatePayment")
	}
	return &paymentsv1.NormalizeRefundResponse{
		Refund: &paymentsv1.Refund{
			Metadata: estimatePaymentReply.GetPayment().GetMetadata(),
			RefundId: req.GetRefundId(),
			Number:   req.GetRefund().GetNumber(),
		},
		Result: &commonv1.OperationResult{
			Message: estimatePaymentReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// ExplainRefundDecision explains a refund.
func (s *RefundValidationServer) ExplainRefundDecision(ctx context.Context, req *paymentsv1.ExplainRefundDecisionRequest) (*paymentsv1.ExplainRefundDecisionResponse, error) {
	if err := s.store.SaveRefund(ctx, req.GetContext().GetTenantId(), req.GetRefund()); err != nil {
		return nil, grpcx.Wrap(err, "save refund")
	}
	s.log.InfoContext(ctx, "explain refund decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRefund().GetAudit().GetEtag())
	for _, line := range req.GetRefund().GetLines() {
		if _, err := s.paymentCalculationClient.ExplainPaymentCalculation(ctx, &paymentsv1.ExplainPaymentCalculationRequest{
			Context: req.GetContext(),
			Payment: &paymentsv1.Payment{
				Lines: []*paymentsv1.PaymentLine{&paymentsv1.PaymentLine{
					Tax: &paymentsv1.PaymentLineTax{
						Amount:       line.GetTax().GetAmount(),
						Exemptions:   line.GetTax().GetExemptions(),
						Jurisdiction: line.GetTax().GetJurisdiction(),
					},
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/ExplainPaymentCalculation")
		}
	}
	recordSessionVersionRequest := &identityv1.RecordSessionVersionRequest{Context: req.GetContext()}
	recordSessionVersionReply := new(identityv1.RecordSessionVersionResponse)
	if err := grpcx.Invoke(ctx, s.conn, identityv1.SessionHistoryService_RecordSessionVersion_FullMethodName, recordSessionVersionRequest, recordSessionVersionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionHistoryService/RecordSessionVersion")
	}
	validatePaymentRequest := &paymentsv1.ValidatePaymentRequest{}
	validatePaymentRequest.Context = req.GetContext()
	validatePaymentRequest.Details = &paymentsv1.ValidatePaymentRequest_Details{
		Contact: &commonv1.ContactInfo{
			Name: recordSessionVersionReply.GetSession().GetRequester().GetName(),
		},
	}
	validatePaymentReply, err := s.paymentValidationClient.ValidatePayment(ctx, validatePaymentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentValidationService/ValidatePayment")
	}
	return &paymentsv1.ExplainRefundDecisionResponse{
		Refund: &paymentsv1.Refund{
			Counterparty: &customersv1.CustomerRef{
				Email: recordSessionVersionReply.GetSession().GetAssignee().GetEmail(),
			},
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: validatePaymentReply.GetPayment().GetBillingAddress().GetPostalCode(),
			},
			Metadata: validatePaymentReply.GetPayment().GetMetadata(),
			RefundId: req.GetRefundId(),
			Total:    req.GetRefund().GetTotal(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScoreRefund scores a refund.
func (s *RefundValidationServer) ScoreRefund(ctx context.Context, req *paymentsv1.ScoreRefundRequest) (*paymentsv1.ScoreRefundResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRefund(ctx, req.GetContext().GetTenantId(), req.GetRefund()); err != nil {
		return nil, grpcx.Wrap(err, "save refund")
	}
	s.log.InfoContext(ctx, "score refund", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRefund().GetAudit().GetEtag())
	var accountHolder string
	switch choice := req.GetRefund().GetSettlement().(type) {
	case *paymentsv1.Refund_Card:
		accountHolder = choice.Card.GetHolderName()
	case *paymentsv1.Refund_Bank:
		accountHolder = choice.Bank.GetAccountHolder()
	case *paymentsv1.Refund_VoucherCode:
		accountHolder = choice.VoucherCode
	}
	ctx = tenancy.With(ctx, "country_code", req.GetDetails().GetDestination().GetCountryCode())
	recalculatePaymentsReply, err := s.paymentCalculationClient.RecalculatePayments(ctx, &paymentsv1.RecalculatePaymentsRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.RecalculatePaymentsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality:    req.GetDetails().GetDestination().GetLocality(),
				CountryCode: tenancy.Value(ctx, "country_code"),
				Recipient:   req.GetRefund().GetBillingAddress().GetRecipient(),
			},
		},
		Payment: &paymentsv1.Payment{
			Settlement: &paymentsv1.Payment_Bank{Bank: &paymentsv1.PaymentBankSettlement{AccountHolder: accountHolder}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/RecalculatePayments")
	}
	sellerListingValidation, ok := s.clients.Get("acme.sellers.v1.SellerListingValidationService").(sellersv1.SellerListingValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.sellers.v1.SellerListingValidationService")
	}
	encoded, err := protojson.Marshal(req.GetRefund())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded paymentsv1.Refund
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	lintSellerListingsRequest := &sellersv1.LintSellerListingsRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: recalculatePaymentsReply.GetPayment().GetAudit().GetUpdatedBy(),
				CreatedBy: (&decoded).GetAudit().GetCreatedBy(),
			},
		},
	}
	lintSellerListingsReply, err := sellerListingValidation.LintSellerListings(ctx, lintSellerListingsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingValidationService/LintSellerListings")
	}
	lintSynonymsRequest := &searchv1.LintSynonymsRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: recalculatePaymentsReply.GetPayment().GetAudit().GetUpdatedBy(),
			},
		},
	}
	lintSynonymsReply, err := grpcx.NewClient(s.conn, searchv1.NewSynonymValidationServiceClient).LintSynonyms(ctx, lintSynonymsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/LintSynonyms")
	}
	return &paymentsv1.ScoreRefundResponse{
		Refund: &paymentsv1.Refund{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: recalculatePaymentsReply.GetPayment().GetBillingAddress().GetCountryCode(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: lintSellerListingsReply.GetSellerListing().GetAudit().GetCreatedBy(),
			},
			RefundId: req.GetRefundId(),
			Total:    req.GetRefund().GetTotal(),
		},
		Result: &commonv1.OperationResult{
			Status:  lintSellerListingsReply.GetResult().GetStatus(),
			Message: lintSynonymsReply.GetOutcome().GetMessage(),
		},
	}, nil
}

// GetRefundRules returns a refund.
func (s *RefundValidationServer) GetRefundRules(ctx context.Context, req *paymentsv1.GetRefundRulesRequest) (*paymentsv1.GetRefundRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetRefundRules", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "refund", req.GetRefundId(), "get_refund_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get refund rules", "tenant", req.GetContext().GetTenantId())
	explainPaymentCalculationRequest := &paymentsv1.ExplainPaymentCalculationRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Counterparty: &paymentsv1.RefundRef{RefundId: req.GetRefundId()},
		},
		Details: &paymentsv1.ExplainPaymentCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
	}
	explainPaymentCalculationReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*paymentsv1.ExplainPaymentCalculationResponse, error) {
		return s.paymentCalculationClient.ExplainPaymentCalculation(ctx, explainPaymentCalculationRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/ExplainPaymentCalculation")
	}
	return &paymentsv1.GetRefundRulesResponse{
		Refund: &paymentsv1.Refund{
			Audit: &commonv1.AuditInfo{
				Etag: explainPaymentCalculationReply.GetPayment().GetAudit().GetEtag(),
			},
			RefundId: req.GetRefundId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
