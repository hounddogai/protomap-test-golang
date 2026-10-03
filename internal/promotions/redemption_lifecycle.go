package promotions

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	formsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/forms/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// RedemptionLifecycleServer implements `acme.promotions.v1.RedemptionLifecycleService`.
// Moves a redemption between the states of its lifecycle.
type RedemptionLifecycleServer struct {
	promotionsv1.UnsafeRedemptionLifecycleServiceServer
	store                       *Store
	log                         *slog.Logger
	payoutSyncClient            paymentsv1.PayoutSyncServiceClient
	redemptionReportingClient   promotionsv1.RedemptionReportingServiceClient
	cartApprovalClient          cartv1.CartApprovalServiceClient
	synonymSyncClient           searchv1.SynonymSyncServiceClient
	formClient                  formsv1.FormServiceClient
	promotionQueryClient        promotionsv1.PromotionQueryServiceClient
	conn                        *grpc.ClientConn
	couponReportingOnce         sync.Once
	couponReportingCached       promotionsv1.CouponReportingServiceClient
	sellerLifecycleClient       sellersv1.SellerLifecycleServiceClient
	pushPayoutFn                func(context.Context, *paymentsv1.PushPayoutRequest, ...grpc.CallOption) (*paymentsv1.PushPayoutResponse, error)
	campaignSyncClient          promotionsv1.CampaignSyncServiceClient
	campaignSyncClientFallback  promotionsv1.CampaignSyncServiceClient
	paymentInstrumentSyncClient paymentsv1.PaymentInstrumentSyncServiceClient
}

// NewRedemptionLifecycleServer wires a RedemptionLifecycleServer.
func NewRedemptionLifecycleServer(deps platform.Deps) *RedemptionLifecycleServer {
	payoutSync := paymentsv1.NewPayoutSyncServiceClient(deps.Conn)
	return &RedemptionLifecycleServer{
		store:                       NewStore(deps.DB),
		log:                         deps.Logger.With("service", "acme.promotions.v1.RedemptionLifecycleService"),
		payoutSyncClient:            paymentsv1.NewPayoutSyncServiceClient(deps.Conn),
		redemptionReportingClient:   promotionsv1.NewRedemptionReportingServiceClient(deps.Conn),
		cartApprovalClient:          cartv1.NewCartApprovalServiceClient(deps.Conn),
		synonymSyncClient:           searchv1.NewSynonymSyncServiceClient(deps.Conn),
		formClient:                  formsv1.NewFormServiceClient(deps.Conn),
		promotionQueryClient:        promotionsv1.NewPromotionQueryServiceClient(deps.Conn),
		conn:                        deps.Conn,
		sellerLifecycleClient:       sellersv1.NewSellerLifecycleServiceClient(deps.Conn),
		pushPayoutFn:                payoutSync.PushPayout,
		campaignSyncClient:          promotionsv1.NewCampaignSyncServiceClient(deps.Conn),
		campaignSyncClientFallback:  promotionsv1.NewCampaignSyncServiceClient(deps.Conn),
		paymentInstrumentSyncClient: paymentsv1.NewPaymentInstrumentSyncServiceClient(deps.Conn),
	}
}

// ActivateRedemption activates a redemption.
func (s *RedemptionLifecycleServer) ActivateRedemption(ctx context.Context, req *promotionsv1.ActivateRedemptionRequest) (*promotionsv1.ActivateRedemptionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "redemption", req.GetRedemptionId(), "activate_redemption"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate redemption", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "display_name", req.GetDetails().GetNewOwner().GetDisplayName())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	resolvePayoutConflictRequest := &paymentsv1.ResolvePayoutConflictRequest{
		Context: req.GetContext(),
		Payout: &paymentsv1.Payout{
			Counterparty: &paymentsv1.RefundRef{DisplayName: tenancy.Value(ctx, "display_name")},
			Audit:        &commonv1.AuditInfo{UpdatedAt: req.GetDetails().GetEffectiveAt()},
		},
		Annotations: annotations,
		Reason:      req.GetReason(),
		Details:     &paymentsv1.ResolvePayoutConflictRequest_Details{Payload: snapshot},
	}
	resolvePayoutConflictReply, err := paymentsv1.PayoutSyncServiceClient.ResolvePayoutConflict(s.payoutSyncClient, ctx, resolvePayoutConflictRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutSyncService/ResolvePayoutConflict")
	}
	streamRedemptionMetricsStream, err := s.redemptionReportingClient.StreamRedemptionMetrics(ctx, &promotionsv1.StreamRedemptionMetricsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionReportingService/StreamRedemptionMetrics")
	}
	for {
		received, err := streamRedemptionMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionReportingService/StreamRedemptionMetrics")
		}
		_ = received
	}
	ctx = tenancy.With(ctx, "display_name", req.GetDetails().GetNewOwner().GetDisplayName())
	submitCartRequest := &cartv1.SubmitCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			Counterparty: &cartv1.SavedListRef{DisplayName: tenancy.Value(ctx, "display_name")},
			Settlement: &cartv1.Cart_Card{Card: &cartv1.CartCardSettlement{
				Network: resolvePayoutConflictReply.GetPayout().GetCard().GetNetwork(),
			}},
		},
	}
	submitCartReply, err := grpcx.Call(ctx, s.cartApprovalClient.SubmitCart, submitCartRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartApprovalService/SubmitCart")
	}
	pushSynonymReply, err := s.synonymSyncClient.PushSynonym(ctx, &searchv1.PushSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: submitCartReply.GetCart().GetAudit().GetUpdatedBy(),
			},
			Owner: &identityv1.UserRef{
				UserId:      req.GetDetails().GetNewOwner().GetUserId(),
				DisplayName: submitCartReply.GetCart().GetCounterparty().GetDisplayName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/PushSynonym")
	}
	return &promotionsv1.ActivateRedemptionResponse{
		Result: &commonv1.OperationResult{
			Message: resolvePayoutConflictReply.GetOutcome().GetMessage(),
			Status:  submitCartReply.GetOutcome().GetStatus(),
		},
		Outcome: &promotionsv1.ActivateRedemptionResponse_Outcome{
			EffectiveAt: pushSynonymReply.GetOutcome().GetEffectiveAt(),
			Message:     pushSynonymReply.GetResult().GetMessage(),
		},
		Redemption: &promotionsv1.Redemption{
			RedemptionId: req.GetRedemptionId(),
			Subject: &promotionsv1.CouponRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	}, nil
}

// SuspendRedemption suspends a redemption.
func (s *RedemptionLifecycleServer) SuspendRedemption(ctx context.Context, req *promotionsv1.SuspendRedemptionRequest) (*promotionsv1.SuspendRedemptionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "redemption", req.GetRedemptionId(), "suspend_redemption"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend redemption", "tenant", req.GetContext().GetTenantId())
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	reconcilePayoutsRequest := &paymentsv1.ReconcilePayoutsRequest{}
	reconcilePayoutsRequest.Context = req.GetContext()
	reconcilePayoutsRequest.Payout = &paymentsv1.Payout{
		Counterparty: &paymentsv1.RefundRef{
			DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
		},
	}
	reconcilePayoutsRequest.Details = &paymentsv1.ReconcilePayoutsRequest_Details{Payload: snapshot}
	reconcilePayoutsReply, err := s.payoutSyncClient.ReconcilePayouts(ctx, reconcilePayoutsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutSyncService/ReconcilePayouts")
	}
	searchFormsReply, err := s.formClient.SearchForms(ctx, &formsv1.SearchFormsRequest{
		Context: req.GetContext(),
		Filter: &formsv1.SearchFormsRequest_Filter{
			Region:       reconcilePayoutsReply.GetPayout().GetBillingAddress().GetRegion(),
			CreatedAfter: reconcilePayoutsReply.GetPayout().GetAudit().GetCreatedAt(),
			OwnerId:      reconcilePayoutsReply.GetPayout().GetCounterparty().GetRefundId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.forms.v1.FormService/SearchForms")
	}
	return &promotionsv1.SuspendRedemptionResponse{
		Redemption: &promotionsv1.Redemption{
			Subject: &promotionsv1.CouponRef{
				DisplayName: reconcilePayoutsReply.GetPayout().GetCounterparty().GetDisplayName(),
			},
			RedemptionId: req.GetRedemptionId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: reconcilePayoutsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  searchFormsReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		Outcome: &promotionsv1.SuspendRedemptionResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// TerminateRedemption terminates a redemption.
func (s *RedemptionLifecycleServer) TerminateRedemption(ctx context.Context, req *promotionsv1.TerminateRedemptionRequest) (*promotionsv1.TerminateRedemptionResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "redemption", req.GetRedemptionId(), "terminate_redemption"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate redemption", "tenant", req.GetContext().GetTenantId())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := redemptionLifecycleServerSuggestPromotionsSteps[mode]
	suggestPromotionsRequest := &promotionsv1.SuggestPromotionsRequest{
		Context: req.GetContext(),
		Filter: &promotionsv1.SuggestPromotionsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			Labels:       req.GetAnnotations(),
			OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
		},
	}
	suggestPromotionsReply, err := run(s, ctx, suggestPromotionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionQueryService/SuggestPromotions")
	}
	cloned := proto.Clone(req.GetDetails()).(*promotionsv1.TerminateRedemptionRequest_Details)
	explainCampaignCalculationRequest := &promotionsv1.ExplainCampaignCalculationRequest{
		Context: req.GetContext(),
		Campaign: &promotionsv1.Campaign{
			Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
				Contact: &commonv1.ContactInfo{
					Name:  cloned.GetNotifyContact().GetName(),
					Phone: req.GetDetails().GetNotifyContact().GetPhone(),
				},
			}},
		},
	}
	explainCampaignCalculationReply, err := explainCampaignCalculationCall(ctx, explainCampaignCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignCalculationService/ExplainCampaignCalculation")
	}
	listCouponReportsRequest := &promotionsv1.ListCouponReportsRequest{
		Context: req.GetContext(),
		Filter: &promotionsv1.ListCouponReportsRequest_Filter{
			Region:       explainCampaignCalculationReply.GetCampaign().GetLocation().GetRegion(),
			CreatedAfter: explainCampaignCalculationReply.GetCampaign().GetAudit().GetCreatedAt(),
		},
	}
	listCouponReportsReply, err := s.couponReportingLazy().ListCouponReports(ctx, listCouponReportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponReportingService/ListCouponReports")
	}
	return &promotionsv1.TerminateRedemptionResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: suggestPromotionsReply.GetResult().GetCorrelationId(),
			Message:       explainCampaignCalculationReply.GetOutcome().GetMessage(),
			Details:       listCouponReportsReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		Redemption: &promotionsv1.Redemption{RedemptionId: req.GetRedemptionId()},
		Outcome:    &promotionsv1.TerminateRedemptionResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// RenewRedemption renews a redemption.
func (s *RedemptionLifecycleServer) RenewRedemption(ctx context.Context, req *promotionsv1.RenewRedemptionRequest) (*promotionsv1.RenewRedemptionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "redemption", req.GetRedemptionId(), "renew_redemption"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew redemption", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*promotionsv1.RenewRedemptionRequest_Details)
	listRedemptionReportsRequest := &promotionsv1.ListRedemptionReportsRequest{
		Context: req.GetContext(),
		Filter:  &promotionsv1.ListRedemptionReportsRequest_Filter{CreatedAfter: cloned.GetEffectiveAt()},
	}
	listRedemptionReportsReply := new(promotionsv1.ListRedemptionReportsResponse)
	if err := grpcx.Invoke(ctx, s.conn, promotionsv1.RedemptionReportingService_ListRedemptionReports_FullMethodName, listRedemptionReportsRequest, listRedemptionReportsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionReportingService/ListRedemptionReports")
	}
	return &promotionsv1.RenewRedemptionResponse{
		Result: &commonv1.OperationResult{
			Status:  listRedemptionReportsReply.GetResult().GetStatus(),
			Details: listRedemptionReportsReply.GetResult().GetDetails(),
		},
		Redemption: &promotionsv1.Redemption{RedemptionId: req.GetRedemptionId()},
		Outcome:    &promotionsv1.RenewRedemptionResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ExpireRedemption expires a redemption.
func (s *RedemptionLifecycleServer) ExpireRedemption(ctx context.Context, req *promotionsv1.ExpireRedemptionRequest) (*promotionsv1.ExpireRedemptionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "redemption", req.GetRedemptionId(), "expire_redemption"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire redemption", "tenant", req.GetContext().GetTenantId())
	scheduleSellerExpiryRequest := &sellersv1.ScheduleSellerExpiryRequest{}
	scheduleSellerExpiryRequest.Context = req.GetContext()
	scheduleSellerExpiryRequest.Details = &sellersv1.ScheduleSellerExpiryRequest_Details{
		Notify: &commonv1.ContactInfo{
			Email: req.GetDetails().GetNotifyContact().GetEmail(),
			Phone: req.GetDetails().GetNotifyContact().GetPhone(),
		},
	}
	scheduleSellerExpiryReply, err := s.sellerLifecycleClient.ScheduleSellerExpiry(ctx, scheduleSellerExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerLifecycleService/ScheduleSellerExpiry")
	}
	searchMetricSeriesesRequest := &metricsv1.SearchMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter: &metricsv1.SearchMetricSeriesesRequest_Filter{
			Region:       scheduleSellerExpiryReply.GetSeller().GetPrimaryAddress().GetRegion(),
			CreatedAfter: scheduleSellerExpiryReply.GetSeller().GetPassport().GetExpiresAt(),
			Labels:       scheduleSellerExpiryReply.GetResult().GetDetails(),
			OwnerId:      scheduleSellerExpiryReply.GetResult().GetCorrelationId(),
		},
	}
	searchMetricSeriesesReply, err := searchMetricSeriesesCall(ctx, searchMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesService/SearchMetricSerieses")
	}
	return &promotionsv1.ExpireRedemptionResponse{
		Result: &commonv1.OperationResult{Status: scheduleSellerExpiryReply.GetOutcome().GetStatus()},
		Redemption: &promotionsv1.Redemption{
			CorrelationId: searchMetricSeriesesReply.GetResult().GetCorrelationId(),
			RedemptionId:  req.GetRedemptionId(),
		},
		Outcome: &promotionsv1.ExpireRedemptionResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ScheduleRedemptionExpiry schedules a redemption.
func (s *RedemptionLifecycleServer) ScheduleRedemptionExpiry(ctx context.Context, req *promotionsv1.ScheduleRedemptionExpiryRequest) (*promotionsv1.ScheduleRedemptionExpiryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "schedule redemption expiry", "tenant", req.GetContext().GetTenantId())
	snapshot, err := protojson.Marshal(req.GetRedemption())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	var envelope promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	pushPayoutRequest := &paymentsv1.PushPayoutRequest{
		Context: req.GetContext(),
		Payout: &paymentsv1.Payout{
			Lines: []*paymentsv1.PayoutLine{&paymentsv1.PayoutLine{Attributes: req.GetRedemption().GetAttributes()}},
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: req.GetRedemption().GetSubject().GetDisplayName(),
			},
		},
		Details:  &paymentsv1.PushPayoutRequest_Details{Payload: snapshot},
		PayoutId: envelope.GetDisplayName(),
	}
	pushPayoutReply, err := s.pushPayoutFn(ctx, pushPayoutRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutSyncService/PushPayout")
	}
	routes := map[string]func(context.Context, *promotionsv1.SyncCampaignsRequest, ...grpc.CallOption) (*promotionsv1.SyncCampaignsResponse, error){
		"primary":  s.campaignSyncClient.SyncCampaigns,
		"fallback": s.campaignSyncClientFallback.SyncCampaigns,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	syncCampaignsRequest := &promotionsv1.SyncCampaignsRequest{
		Context: req.GetContext(),
		Campaign: &promotionsv1.Campaign{
			Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
				Contact: &commonv1.ContactInfo{
					Email: req.GetDetails().GetNotify().GetEmail(),
					Name:  req.GetDetails().GetNotify().GetName(),
				},
			}},
			Location: &commonv1.PostalAddress{
				CountryCode: pushPayoutReply.GetPayout().GetBillingAddress().GetCountryCode(),
			},
		},
	}
	syncCampaignsReply, err := route(ctx, syncCampaignsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignSyncService/SyncCampaigns")
	}
	var envelope2 promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	syncPaymentInstrumentsReply, err := s.paymentInstrumentSyncClient.SyncPaymentInstruments(ctx, &paymentsv1.SyncPaymentInstrumentsRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: syncCampaignsReply.GetCampaign().GetAudit().GetUpdatedBy(),
			},
			Funding: &paymentsv1.PaymentInstrument_Bank{Bank: &paymentsv1.PaymentInstrumentBankFunding{
				AccountHolder: pushPayoutReply.GetPayout().GetBank().GetAccountHolder(),
			}},
		},
		PaymentInstrumentId: envelope2.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentSyncService/SyncPaymentInstruments")
	}
	return &promotionsv1.ScheduleRedemptionExpiryResponse{
		Outcome: &promotionsv1.ScheduleRedemptionExpiryResponse_Outcome{Message: pushPayoutReply.GetResult().GetMessage()},
		Result: &commonv1.OperationResult{
			ReferenceIds:  syncCampaignsReply.GetResult().GetReferenceIds(),
			CorrelationId: syncPaymentInstrumentsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Redemption: &promotionsv1.Redemption{
			CorrelationId: syncCampaignsReply.GetResult().GetCorrelationId(),
			RedemptionId:  req.GetRedemptionId(),
			Source:        req.GetRedemption().GetSource(),
		},
	}, nil
}

// redemptionLifecycleServerSuggestPromotionsSteps are the ways to run
// acme.promotions.v1.PromotionQueryService/SuggestPromotions, by name.
var redemptionLifecycleServerSuggestPromotionsSteps = map[string]func(*RedemptionLifecycleServer, context.Context, *promotionsv1.SuggestPromotionsRequest) (*promotionsv1.SuggestPromotionsResponse, error){
	"default": (*RedemptionLifecycleServer).suggestPromotionsStep,
	"audited": (*RedemptionLifecycleServer).suggestPromotionsAuditedStep,
}

func (s *RedemptionLifecycleServer) suggestPromotionsStep(ctx context.Context, request *promotionsv1.SuggestPromotionsRequest) (*promotionsv1.SuggestPromotionsResponse, error) {
	return s.promotionQueryClient.SuggestPromotions(ctx, request)
}

func (s *RedemptionLifecycleServer) suggestPromotionsAuditedStep(ctx context.Context, request *promotionsv1.SuggestPromotionsRequest) (*promotionsv1.SuggestPromotionsResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.promotions.v1.PromotionQueryService/SuggestPromotions")
	return s.promotionQueryClient.SuggestPromotions(ctx, request)
}

// couponReportingLazy dials acme.promotions.v1.CouponReportingService the first time a handler needs it.
func (s *RedemptionLifecycleServer) couponReportingLazy() promotionsv1.CouponReportingServiceClient {
	s.couponReportingOnce.Do(func() { s.couponReportingCached = promotionsv1.NewCouponReportingServiceClient(s.conn) })
	return s.couponReportingCached
}
