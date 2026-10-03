package cart

import (
	"context"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// SavedListCalculationServer implements `acme.cart.v1.SavedListCalculationService`.
// Computes the figures of a saved list, such as totals, taxes, and estimates.
type SavedListCalculationServer struct {
	cartv1.UnimplementedSavedListCalculationServiceServer
	store                   *Store
	log                     *slog.Logger
	synonymValidationClient searchv1.SynonymValidationServiceClient
	cartCalculationClient   cartv1.CartCalculationServiceClient
	paymentValidationClient paymentsv1.PaymentValidationServiceClient
	synonymSyncClient       searchv1.SynonymSyncServiceClient
	refundQueryClient       paymentsv1.RefundQueryServiceClient
	paymentValidationPort   paymentValidationPort
	cartCalculationPort     cartCalculationPort
	auditEventQueryClient   auditv1.AuditEventQueryServiceClient
	simulateCartFn          func(context.Context, *cartv1.SimulateCartRequest, ...grpc.CallOption) (*cartv1.SimulateCartResponse, error)
	metricSeriesClient      metricsv1.MetricSeriesServiceClient
	conn                    *grpc.ClientConn
	cartCalculationOnce     sync.Once
	cartCalculationCached   cartv1.CartCalculationServiceClient
	paymentValidationOnce   sync.Once
	paymentValidationCached paymentsv1.PaymentValidationServiceClient
}

// NewSavedListCalculationServer wires a SavedListCalculationServer.
func NewSavedListCalculationServer(deps platform.Deps) *SavedListCalculationServer {
	cartCalculation := cartv1.NewCartCalculationServiceClient(deps.Conn)
	return &SavedListCalculationServer{
		store:                   NewStore(deps.DB),
		log:                     deps.Logger.With("service", "acme.cart.v1.SavedListCalculationService"),
		synonymValidationClient: searchv1.NewSynonymValidationServiceClient(deps.Conn),
		cartCalculationClient:   cartv1.NewCartCalculationServiceClient(deps.Conn),
		paymentValidationClient: paymentsv1.NewPaymentValidationServiceClient(deps.Conn),
		synonymSyncClient:       searchv1.NewSynonymSyncServiceClient(deps.Conn),
		refundQueryClient:       paymentsv1.NewRefundQueryServiceClient(deps.Conn),
		paymentValidationPort:   newPaymentValidationPort(deps),
		cartCalculationPort:     newCartCalculationPort(deps),
		auditEventQueryClient:   auditv1.NewAuditEventQueryServiceClient(deps.Conn),
		simulateCartFn:          cartCalculation.SimulateCart,
		metricSeriesClient:      metricsv1.NewMetricSeriesServiceClient(deps.Conn),
		conn:                    deps.Conn,
	}
}

// CalculateSavedList calculates a saved list.
func (s *SavedListCalculationServer) CalculateSavedList(ctx context.Context, req *cartv1.CalculateSavedListRequest) (*cartv1.CalculateSavedListResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "calculate saved list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := savedListCalculationServerScoreSynonymSteps[mode]
	scoreSynonymRequest := &searchv1.ScoreSynonymRequest{
		Context: req.GetContext(),
		Details: &searchv1.ScoreSynonymRequest_Details{
			Destination: &commonv1.PostalAddress{
				Lines:      req.GetSavedList().GetBillingAddress().GetLines(),
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
			},
		},
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetSavedList().GetAudit().GetUpdatedBy(),
				CreatedBy: req.GetSavedList().GetAudit().GetCreatedBy(),
			},
		},
	}
	scoreSynonymReply, err := run(s, ctx, scoreSynonymRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/ScoreSynonym")
	}
	mode2 := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode2 = "audited"
	}
	run2 := savedListCalculationServerExplainCartCalculationSteps[mode2]
	cloned := proto.Clone(req.GetDetails()).(*cartv1.CalculateSavedListRequest_Details)
	explainCartCalculationRequest := &cartv1.ExplainCartCalculationRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			Lines: []*cartv1.CartLine{&cartv1.CartLine{
				Amount: scoreSynonymReply.GetSynonym().GetDiscount().GetAmount(),
			}},
			Settlement:     &cartv1.Cart_Bank{Bank: &cartv1.CartBankSettlement{Bic: req.GetSavedList().GetBank().GetBic()}},
			BillingAddress: &commonv1.PostalAddress{Recipient: cloned.GetDestination().GetRecipient()},
		},
	}
	explainCartCalculationReply, err := run2(s, ctx, explainCartCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartCalculationService/ExplainCartCalculation")
	}
	ctx = tenancy.With(ctx, "created_by", req.GetSavedList().GetAudit().GetCreatedBy())
	normalizePaymentRequest := &paymentsv1.NormalizePaymentRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: req.GetSavedList().GetCounterparty().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{CreatedBy: tenancy.Value(ctx, "created_by")},
		},
	}
	normalizePaymentReply, err := grpcx.Call(ctx, s.paymentValidationClient.NormalizePayment, normalizePaymentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentValidationService/NormalizePayment")
	}
	return &cartv1.CalculateSavedListResponse{
		SavedList: &cartv1.SavedList{
			Lines: []*cartv1.SavedListLine{&cartv1.SavedListLine{
				Amount: scoreSynonymReply.GetSynonym().GetDiscount().GetAmount(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: scoreSynonymReply.GetSynonym().GetAudit().GetCreatedBy(),
			},
			Number:      explainCartCalculationReply.GetCart().GetCounterparty().GetNumber(),
			Notes:       normalizePaymentReply.GetPayment().GetNotes(),
			SavedListId: req.GetSavedListId(),
			Status:      req.GetSavedList().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EstimateSavedList estimates a saved list.
func (s *SavedListCalculationServer) EstimateSavedList(ctx context.Context, req *cartv1.EstimateSavedListRequest) (*cartv1.EstimateSavedListResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "estimate saved list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	for line := range iterx.Each(req.GetSavedList().GetLines(), func(candidate *cartv1.SavedListLine) bool { return candidate != nil }) {
		if _, err := s.cartCalculationClient.SimulateCart(ctx, &cartv1.SimulateCartRequest{
			Context: req.GetContext(),
			Cart: &cartv1.Cart{
				Lines: []*cartv1.CartLine{&cartv1.CartLine{
					Tax: &cartv1.CartLineTax{
						Jurisdiction: line.GetTax().GetJurisdiction(),
						Exemptions:   line.GetTax().GetExemptions(),
						Amount:       line.GetAmount(),
					},
					Attributes: line.GetAttributes(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.cart.v1.CartCalculationService/SimulateCart")
		}
	}
	savedListCache := cache.New[string, *cartv1.SavedList](1)
	savedListCache.Put(req.GetContext().GetTraceId(), req.GetSavedList())
	cachedSavedList, _ := savedListCache.Get(req.GetContext().GetTraceId())
	pullSynonymsRequest := &searchv1.PullSynonymsRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetSavedList().GetCounterparty().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetSavedList().GetAudit().GetUpdatedBy(),
				UpdatedAt: cachedSavedList.GetAudit().GetUpdatedAt(),
			},
		},
	}
	pullSynonymsReply, err := searchv1.SynonymSyncServiceClient.PullSynonyms(s.synonymSyncClient, ctx, pullSynonymsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/PullSynonyms")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*cartv1.CompareCartOptionsResponse, len(req.GetSavedList().GetAttachments()))
	for i, attachment := range req.GetSavedList().GetAttachments() {
		group.Go(func() error {
			reply, err := s.cartCalculationClient.CompareCartOptions(groupCtx, &cartv1.CompareCartOptionsRequest{
				Context: req.GetContext(),
				Cart: &cartv1.Cart{
					Attachments: []*cartv1.CartAttachment{&cartv1.CartAttachment{
						Uri:      attachment.GetUri(),
						FileName: attachment.GetFileName(),
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
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartCalculationService/CompareCartOptions")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	findRefundsRequest := buildFindRefundsRequestForEstimateSavedList(req)
	findRefundsReply, err := s.refundQueryClient.FindRefunds(ctx, findRefundsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundQueryService/FindRefunds")
	}
	return &cartv1.EstimateSavedListResponse{
		SavedList: &cartv1.SavedList{
			Lines: []*cartv1.SavedListLine{&cartv1.SavedListLine{
				Description: pullSynonymsReply.GetSynonym().GetDescription(),
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: pullSynonymsReply.GetSynonym().GetAudit().GetUpdatedBy(),
			},
			SavedListId: req.GetSavedListId(),
			Notes:       req.GetSavedList().GetNotes(),
		},
		Result: &commonv1.OperationResult{Status: findRefundsReply.GetResult().GetStatus()},
	}, nil
}

// QuoteSavedList quotes a saved list.
func (s *SavedListCalculationServer) QuoteSavedList(ctx context.Context, req *cartv1.QuoteSavedListRequest) (*cartv1.QuoteSavedListResponse, error) {
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "quote saved list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	explainPaymentDecisionRequest := &paymentsv1.ExplainPaymentDecisionRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetDetails().GetDestination().GetRegion()},
		},
	}
	explainPaymentDecisionReply, err := s.paymentValidationPort.ExplainPaymentDecision(ctx, explainPaymentDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentValidationService/ExplainPaymentDecision")
	}
	var last4 string
	switch choice := req.GetSavedList().GetSettlement().(type) {
	case *cartv1.SavedList_Card:
		last4 = choice.Card.GetLast4()
	case *cartv1.SavedList_Bank:
		last4 = choice.Bank.GetIban()
	case *cartv1.SavedList_VoucherCode:
		last4 = choice.VoucherCode
	}
	calculateCartRequest := &cartv1.CalculateCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			Settlement: &cartv1.Cart_Card{Card: &cartv1.CartCardSettlement{
				HolderName: explainPaymentDecisionReply.GetPayment().GetCard().GetHolderName(),
				Last4:      last4,
			}},
			BillingAddress: &commonv1.PostalAddress{
				Lines:      explainPaymentDecisionReply.GetPayment().GetBillingAddress().GetLines(),
				PostalCode: explainPaymentDecisionReply.GetPayment().GetBillingAddress().GetPostalCode(),
			},
		},
	}
	calculateCartReply, err := s.cartCalculationPort.CalculateCart(ctx, calculateCartRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartCalculationService/CalculateCart")
	}
	suggestAuditEventsRequest := &auditv1.SuggestAuditEventsRequest{
		Context: req.GetContext(),
		Filter: &auditv1.SuggestAuditEventsRequest_Filter{
			Region:       explainPaymentDecisionReply.GetPayment().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetSavedList().GetAudit().GetCreatedAt(),
		},
	}
	suggestAuditEventsReply, err := auditv1.AuditEventQueryServiceClient.SuggestAuditEvents(s.auditEventQueryClient, ctx, suggestAuditEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventQueryService/SuggestAuditEvents")
	}
	_ = suggestAuditEventsReply
	return &cartv1.QuoteSavedListResponse{
		SavedList: &cartv1.SavedList{
			BillingAddress: &commonv1.PostalAddress{
				Locality: explainPaymentDecisionReply.GetPayment().GetBillingAddress().GetLocality(),
			},
			Metadata:     explainPaymentDecisionReply.GetPayment().GetMetadata(),
			SavedListId:  req.GetSavedListId(),
			Counterparty: req.GetSavedList().GetCounterparty(),
		},
		Result: &commonv1.OperationResult{
			Details: calculateCartReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// CompareSavedListOptions compares a saved list.
func (s *SavedListCalculationServer) CompareSavedListOptions(ctx context.Context, req *cartv1.CompareSavedListOptionsRequest) (*cartv1.CompareSavedListOptionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "compare saved list options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	simulateCartRequest := &cartv1.SimulateCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetSavedList().GetBillingAddress().GetCountryCode(),
			},
			Settlement: &cartv1.Cart_Card{Card: &cartv1.CartCardSettlement{HolderName: req.GetSavedList().GetCard().GetHolderName()}},
			Audit:      &commonv1.AuditInfo{UpdatedBy: req.GetSavedList().GetAudit().GetUpdatedBy()},
		},
	}
	simulateCartReply, err := s.simulateCartFn(ctx, simulateCartRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartCalculationService/SimulateCart")
	}
	cloneMetricSeriesRequest := &metricsv1.CloneMetricSeriesRequest{
		Context: req.GetContext(),
		MetricSeries: &metricsv1.MetricSeries{
			Locator: &metricsv1.MetricSeries_Address{Address: &commonv1.PostalAddress{Recipient: req.GetDetails().GetDestination().GetRecipient()}},
		},
	}
	cloneMetricSeriesReply, err := metricsv1.MetricSeriesServiceClient.CloneMetricSeries(s.metricSeriesClient, ctx, cloneMetricSeriesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesService/CloneMetricSeries")
	}
	estimateCartReply, err := s.cartCalculationClient.EstimateCart(ctx, &cartv1.EstimateCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			Audit: &commonv1.AuditInfo{Etag: simulateCartReply.GetCart().GetAudit().GetEtag()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartCalculationService/EstimateCart")
	}
	_ = estimateCartReply
	return &cartv1.CompareSavedListOptionsResponse{
		Result: &commonv1.OperationResult{
			Status:       simulateCartReply.GetResult().GetStatus(),
			ReferenceIds: cloneMetricSeriesReply.GetResult().GetReferenceIds(),
		},
		SavedList: &cartv1.SavedList{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: cloneMetricSeriesReply.GetMetricSeries().GetAddress().GetRecipient(),
			},
			SavedListId: req.GetSavedListId(),
			DueAt:       req.GetSavedList().GetDueAt(),
		},
	}, nil
}

// RecalculateSavedLists recalculates a saved list.
func (s *SavedListCalculationServer) RecalculateSavedLists(ctx context.Context, req *cartv1.RecalculateSavedListsRequest) (*cartv1.RecalculateSavedListsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "recalculate saved lists", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	var last4 string
	switch choice := req.GetSavedList().GetSettlement().(type) {
	case *cartv1.SavedList_Card:
		last4 = choice.Card.GetLast4()
	case *cartv1.SavedList_Bank:
		last4 = choice.Bank.GetIban()
	case *cartv1.SavedList_VoucherCode:
		last4 = choice.VoucherCode
	}
	estimateCartRequest := &cartv1.EstimateCartRequest{
		Context: req.GetContext(),
		Details: &cartv1.EstimateCartRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region: req.GetSavedList().GetBillingAddress().GetRegion(),
				Lines:  req.GetDetails().GetDestination().GetLines(),
			},
		},
		Cart: &cartv1.Cart{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: req.GetSavedList().GetBillingAddress().GetRecipient(),
			},
			Settlement: &cartv1.Cart_Card{Card: &cartv1.CartCardSettlement{Last4: last4}},
		},
	}
	estimateCartReply, err := s.cartCalculationLazy().EstimateCart(ctx, estimateCartRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartCalculationService/EstimateCart")
	}
	return &cartv1.RecalculateSavedListsResponse{
		SavedList: &cartv1.SavedList{
			Notes:       estimateCartReply.GetCart().GetNotes(),
			SavedListId: req.GetSavedListId(),
			Total:       req.GetSavedList().GetTotal(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SimulateSavedList simulates a saved list.
func (s *SavedListCalculationServer) SimulateSavedList(ctx context.Context, req *cartv1.SimulateSavedListRequest) (*cartv1.SimulateSavedListResponse, error) {
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "simulate saved list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	calculateCartRequest := &cartv1.CalculateCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			Counterparty: &cartv1.SavedListRef{
				DisplayName: req.GetSavedList().GetCounterparty().GetDisplayName(),
			},
			Settlement: &cartv1.Cart_Card{Card: &cartv1.CartCardSettlement{HolderName: req.GetSavedList().GetCard().GetHolderName()}},
		},
		Details: &cartv1.CalculateCartRequest_Details{
			Destination: &commonv1.PostalAddress{Lines: req.GetDetails().GetDestination().GetLines()},
		},
	}
	calculateCartReply, err := grpcx.Call(ctx, s.cartCalculationClient.CalculateCart, calculateCartRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartCalculationService/CalculateCart")
	}
	pushSynonymReply, err := s.synonymSyncClient.PushSynonym(ctx, &searchv1.PushSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Audit:   &commonv1.AuditInfo{UpdatedBy: req.GetSavedList().GetAudit().GetUpdatedBy()},
			Version: req.GetSavedList().GetCounterparty().GetVersion(),
			Owner: &identityv1.UserRef{
				DisplayName: calculateCartReply.GetCart().GetCounterparty().GetDisplayName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/PushSynonym")
	}
	detailsCache := cache.New[string, *cartv1.SimulateSavedListRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	simulateCartRequest := &cartv1.SimulateCartRequest{
		Context: req.GetContext(),
		Details: &cartv1.SimulateCartRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality: calculateCartReply.GetCart().GetBillingAddress().GetLocality(),
				Lines:    cachedDetails.GetDestination().GetLines(),
			},
		},
		Cart: &cartv1.Cart{
			Settlement:     &cartv1.Cart_Card{Card: &cartv1.CartCardSettlement{HolderName: req.GetSavedList().GetCard().GetHolderName()}},
			BillingAddress: &commonv1.PostalAddress{Region: req.GetDetails().GetDestination().GetRegion()},
		},
	}
	simulateCartReply := new(cartv1.SimulateCartResponse)
	if err := grpcx.Invoke(ctx, s.conn, cartv1.CartCalculationService_SimulateCart_FullMethodName, simulateCartRequest, simulateCartReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartCalculationService/SimulateCart")
	}
	return &cartv1.SimulateSavedListResponse{
		SavedList: &cartv1.SavedList{
			DueAt: calculateCartReply.GetCart().GetDueAt(),
			Audit: &commonv1.AuditInfo{
				CreatedAt: pushSynonymReply.GetSynonym().GetAudit().GetCreatedAt(),
			},
			IssuedAt:    simulateCartReply.GetCart().GetIssuedAt(),
			SavedListId: req.GetSavedListId(),
			Total:       req.GetSavedList().GetTotal(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainSavedListCalculation explains a saved list.
func (s *SavedListCalculationServer) ExplainSavedListCalculation(ctx context.Context, req *cartv1.ExplainSavedListCalculationRequest) (*cartv1.ExplainSavedListCalculationResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSavedList(ctx, req.GetContext().GetTenantId(), req.GetSavedList()); err != nil {
		return nil, grpcx.Wrap(err, "save saved list")
	}
	s.log.InfoContext(ctx, "explain saved list calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSavedList().GetAudit().GetEtag())
	for _, attachment := range req.GetSavedList().GetAttachments() {
		if _, err := s.cartCalculationClient.QuoteCart(ctx, &cartv1.QuoteCartRequest{
			Context: req.GetContext(),
			Cart: &cartv1.Cart{
				Attachments: []*cartv1.CartAttachment{&cartv1.CartAttachment{
					ContentType: attachment.GetContentType(),
					Uri:         attachment.GetUri(),
					SizeBytes:   attachment.GetSizeBytes(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.cart.v1.CartCalculationService/QuoteCart")
		}
	}
	jobs := make(chan *cartv1.SavedListLine)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range jobs {
				if _, err := s.cartCalculationClient.ExplainCartCalculation(workCtx, &cartv1.ExplainCartCalculationRequest{
					Context: req.GetContext(),
					Cart: &cartv1.Cart{
						Lines: []*cartv1.CartLine{&cartv1.CartLine{
							Tax: &cartv1.CartLineTax{
								Jurisdiction: line.GetTax().GetJurisdiction(),
								Amount:       line.GetAmount(),
							},
						}},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetSavedList().GetLines() {
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
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartCalculationService/ExplainCartCalculation")
	}
	return &cartv1.ExplainSavedListCalculationResponse{
		SavedList: &cartv1.SavedList{
			SavedListId:    req.GetSavedListId(),
			BillingAddress: req.GetSavedList().GetBillingAddress(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetSavedListRates returns a saved list.
func (s *SavedListCalculationServer) GetSavedListRates(ctx context.Context, req *cartv1.GetSavedListRatesRequest) (*cartv1.GetSavedListRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetSavedListRates", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "saved_list", req.GetSavedListId(), "get_saved_list_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get saved list rates", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "saved_list_id", req.GetSavedListId())
	simulateCartReply, err := s.cartCalculationClient.SimulateCart(ctx, &cartv1.SimulateCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			Counterparty: &cartv1.SavedListRef{SavedListId: tenancy.Value(ctx, "saved_list_id")},
			Audit:        &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
			Metadata:     req.GetContext().GetBaggage(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartCalculationService/SimulateCart")
	}
	scorePaymentRequest := &paymentsv1.ScorePaymentRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Settlement: &paymentsv1.Payment_Card{Card: &paymentsv1.PaymentCardSettlement{
				Last4: simulateCartReply.GetCart().GetCard().GetLast4(),
				Token: simulateCartReply.GetCart().GetCard().GetToken(),
			}},
		},
		Details: &paymentsv1.ScorePaymentRequest_Details{
			Destination: &commonv1.PostalAddress{
				Lines:  simulateCartReply.GetCart().GetBillingAddress().GetLines(),
				Region: simulateCartReply.GetCart().GetBillingAddress().GetRegion(),
			},
		},
	}
	scorePaymentReply, err := s.paymentValidationLazy().ScorePayment(ctx, scorePaymentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentValidationService/ScorePayment")
	}
	return &cartv1.GetSavedListRatesResponse{
		SavedList: &cartv1.SavedList{
			Audit: &commonv1.AuditInfo{
				CreatedBy: simulateCartReply.GetCart().GetAudit().GetCreatedBy(),
			},
			Notes: simulateCartReply.GetCart().GetNotes(),
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: scorePaymentReply.GetPayment().GetBillingAddress().GetCountryCode(),
			},
			IssuedAt:    scorePaymentReply.GetPayment().GetIssuedAt(),
			SavedListId: req.GetSavedListId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// savedListCalculationServerScoreSynonymSteps are the ways to run acme.search.v1.SynonymValidationService/ScoreSynonym,
// by name.
var savedListCalculationServerScoreSynonymSteps = map[string]func(*SavedListCalculationServer, context.Context, *searchv1.ScoreSynonymRequest) (*searchv1.ScoreSynonymResponse, error){
	"default": (*SavedListCalculationServer).scoreSynonymStep,
	"audited": (*SavedListCalculationServer).scoreSynonymAuditedStep,
}

func (s *SavedListCalculationServer) scoreSynonymStep(ctx context.Context, request *searchv1.ScoreSynonymRequest) (*searchv1.ScoreSynonymResponse, error) {
	return s.synonymValidationClient.ScoreSynonym(ctx, request)
}

func (s *SavedListCalculationServer) scoreSynonymAuditedStep(ctx context.Context, request *searchv1.ScoreSynonymRequest) (*searchv1.ScoreSynonymResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.search.v1.SynonymValidationService/ScoreSynonym")
	return s.synonymValidationClient.ScoreSynonym(ctx, request)
}

// savedListCalculationServerExplainCartCalculationSteps are the ways to run
// acme.cart.v1.CartCalculationService/ExplainCartCalculation, by name.
var savedListCalculationServerExplainCartCalculationSteps = map[string]func(*SavedListCalculationServer, context.Context, *cartv1.ExplainCartCalculationRequest) (*cartv1.ExplainCartCalculationResponse, error){
	"default": (*SavedListCalculationServer).explainCartCalculationStep,
	"audited": (*SavedListCalculationServer).explainCartCalculationAuditedStep,
}

func (s *SavedListCalculationServer) explainCartCalculationStep(ctx context.Context, request *cartv1.ExplainCartCalculationRequest) (*cartv1.ExplainCartCalculationResponse, error) {
	return s.cartCalculationClient.ExplainCartCalculation(ctx, request)
}

func (s *SavedListCalculationServer) explainCartCalculationAuditedStep(ctx context.Context, request *cartv1.ExplainCartCalculationRequest) (*cartv1.ExplainCartCalculationResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.cart.v1.CartCalculationService/ExplainCartCalculation")
	return s.cartCalculationClient.ExplainCartCalculation(ctx, request)
}

// cartCalculationLazy dials acme.cart.v1.CartCalculationService the first time a handler needs it.
func (s *SavedListCalculationServer) cartCalculationLazy() cartv1.CartCalculationServiceClient {
	s.cartCalculationOnce.Do(func() { s.cartCalculationCached = cartv1.NewCartCalculationServiceClient(s.conn) })
	return s.cartCalculationCached
}

// paymentValidationLazy dials acme.payments.v1.PaymentValidationService the first time a handler needs it.
func (s *SavedListCalculationServer) paymentValidationLazy() paymentsv1.PaymentValidationServiceClient {
	s.paymentValidationOnce.Do(func() { s.paymentValidationCached = paymentsv1.NewPaymentValidationServiceClient(s.conn) })
	return s.paymentValidationCached
}
