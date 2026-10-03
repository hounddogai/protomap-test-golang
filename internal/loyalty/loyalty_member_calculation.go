package loyalty

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	fuelv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fuel/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// LoyaltyMemberCalculationServer implements `acme.loyalty.v1.LoyaltyMemberCalculationService`.
// Computes the figures of a loyalty member, such as totals, taxes, and estimates.
type LoyaltyMemberCalculationServer struct {
	loyaltyv1.UnimplementedLoyaltyMemberCalculationServiceServer
	store                                 *Store
	log                                   *slog.Logger
	metricSeriesClient                    metricsv1.MetricSeriesServiceClient
	clients                               *registry.Registry
	bus                                   *eventbus.Bus
	notificationValidationClient          notificationsv1.NotificationValidationServiceClient
	giftCardCalculationClient             giftcardsv1.GiftCardCalculationServiceClient
	fuelCardCalculationClient             fuelv1.FuelCardCalculationServiceClient
	conn                                  *grpc.ClientConn
	sellerPayoutScheduleCalculationClient sellersv1.SellerPayoutScheduleCalculationServiceClient
	listSynonymExportsFn                  func(context.Context, *searchv1.ListSynonymExportsRequest, ...grpc.CallOption) (*searchv1.ListSynonymExportsResponse, error)
	reviewModerationQueryClient           reviewsv1.ReviewModerationQueryServiceClient
	giftCardBatchCalculationClient        giftcardsv1.GiftCardBatchCalculationServiceClient
	auditEventClient                      auditv1.AuditEventServiceClient
	sellerListingCalculationClient        sellersv1.SellerListingCalculationServiceClient
	searchIndexValidationClient           searchv1.SearchIndexValidationServiceClient
}

// NewLoyaltyMemberCalculationServer wires a LoyaltyMemberCalculationServer.
func NewLoyaltyMemberCalculationServer(deps platform.Deps) *LoyaltyMemberCalculationServer {
	synonymExport := searchv1.NewSynonymExportServiceClient(deps.Conn)
	return &LoyaltyMemberCalculationServer{
		store:                                 NewStore(deps.DB),
		log:                                   deps.Logger.With("service", "acme.loyalty.v1.LoyaltyMemberCalculationService"),
		metricSeriesClient:                    metricsv1.NewMetricSeriesServiceClient(deps.Conn),
		clients:                               deps.Clients,
		bus:                                   deps.Bus,
		notificationValidationClient:          notificationsv1.NewNotificationValidationServiceClient(deps.Conn),
		giftCardCalculationClient:             giftcardsv1.NewGiftCardCalculationServiceClient(deps.Conn),
		fuelCardCalculationClient:             fuelv1.NewFuelCardCalculationServiceClient(deps.Conn),
		conn:                                  deps.Conn,
		sellerPayoutScheduleCalculationClient: sellersv1.NewSellerPayoutScheduleCalculationServiceClient(deps.Conn),
		listSynonymExportsFn:                  synonymExport.ListSynonymExports,
		reviewModerationQueryClient:           reviewsv1.NewReviewModerationQueryServiceClient(deps.Conn),
		giftCardBatchCalculationClient:        giftcardsv1.NewGiftCardBatchCalculationServiceClient(deps.Conn),
		auditEventClient:                      auditv1.NewAuditEventServiceClient(deps.Conn),
		sellerListingCalculationClient:        sellersv1.NewSellerListingCalculationServiceClient(deps.Conn),
		searchIndexValidationClient:           searchv1.NewSearchIndexValidationServiceClient(deps.Conn),
	}
}

// CalculateLoyaltyMember calculates a loyalty member.
func (s *LoyaltyMemberCalculationServer) CalculateLoyaltyMember(ctx context.Context, req *loyaltyv1.CalculateLoyaltyMemberRequest) (*loyaltyv1.CalculateLoyaltyMemberResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CalculateLoyaltyMember", "error", err)
		return nil, err
	}
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "calculate loyalty member", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*metricsv1.CloneMetricSeriesResponse, len(req.GetLoyaltyMember().GetLimits()))
	for i, limit := range req.GetLoyaltyMember().GetLimits() {
		group.Go(func() error {
			reply, err := s.metricSeriesClient.CloneMetricSeries(groupCtx, &metricsv1.CloneMetricSeriesRequest{
				Context: req.GetContext(),
				MetricSeries: &metricsv1.MetricSeries{
					Name:    limit.GetName(),
					Locator: &metricsv1.MetricSeries_Address{Address: &commonv1.PostalAddress{Locality: req.GetDetails().GetDestination().GetLocality()}},
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
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesService/CloneMetricSeries")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	searchIndexExport, ok := s.clients.Get("acme.search.v1.SearchIndexExportService").(searchv1.SearchIndexExportServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.search.v1.SearchIndexExportService")
	}
	scheduleSearchIndexExportRequest := &searchv1.ScheduleSearchIndexExportRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
	}
	scheduleSearchIndexExportReply, err := searchIndexExport.ScheduleSearchIndexExport(ctx, scheduleSearchIndexExportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/ScheduleSearchIndexExport")
	}
	return &loyaltyv1.CalculateLoyaltyMemberResponse{
		Result: &commonv1.OperationResult{
			Details: scheduleSearchIndexExportReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
			Holder:          req.GetLoyaltyMember().GetHolder(),
		},
	}, nil
}

// EstimateLoyaltyMember estimates a loyalty member.
func (s *LoyaltyMemberCalculationServer) EstimateLoyaltyMember(ctx context.Context, req *loyaltyv1.EstimateLoyaltyMemberRequest) (*loyaltyv1.EstimateLoyaltyMemberResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "estimate loyalty member", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "loyalty.estimate_loyalty_member.check_metric_series_eligibility",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"metric_series.owner.email":      req.GetLoyaltyMember().GetContact().GetEmail(),
			"metric_series.audit.updated_by": req.GetLoyaltyMember().GetAudit().GetUpdatedBy(),
			"context.tenant_id":              req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	scoreNotificationRequest := buildScoreNotificationRequestForEstimateLoyaltyMember(req)
	scoreNotificationReply, err := s.notificationValidationClient.ScoreNotification(ctx, scoreNotificationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationValidationService/ScoreNotification")
	}
	return &loyaltyv1.EstimateLoyaltyMemberResponse{
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Contact: &commonv1.ContactInfo{
				Phone: scoreNotificationReply.GetNotification().GetRequester().GetPhone(),
			},
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
			CurrencyCode:    req.GetDetails().GetCurrencyCode(),
		},
		Breakdown: &loyaltyv1.EstimateLoyaltyMemberResponse_Breakdown{
			CurrencyCode: scoreNotificationReply.GetBreakdown().GetCurrencyCode(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// QuoteLoyaltyMember quotes a loyalty member.
func (s *LoyaltyMemberCalculationServer) QuoteLoyaltyMember(ctx context.Context, req *loyaltyv1.QuoteLoyaltyMemberRequest) (*loyaltyv1.QuoteLoyaltyMemberResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "quote loyalty member", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	jobs := make(chan *loyaltyv1.LoyaltyMemberLimit)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for limit := range jobs {
				if _, err := s.giftCardCalculationClient.RecalculateGiftCards(workCtx, &giftcardsv1.RecalculateGiftCardsRequest{
					Context: req.GetContext(),
					GiftCard: &giftcardsv1.GiftCard{
						Limits: []*giftcardsv1.GiftCardLimit{&giftcardsv1.GiftCardLimit{
							Period: limit.GetPeriod(),
							Amount: limit.GetAmount(),
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
	for _, queued := range req.GetLoyaltyMember().GetLimits() {
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
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/RecalculateGiftCards")
	}
	return &loyaltyv1.QuoteLoyaltyMemberResponse{
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
			Balance:         req.GetLoyaltyMember().GetBalance(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CompareLoyaltyMemberOptions compares a loyalty member.
func (s *LoyaltyMemberCalculationServer) CompareLoyaltyMemberOptions(ctx context.Context, req *loyaltyv1.CompareLoyaltyMemberOptionsRequest) (*loyaltyv1.CompareLoyaltyMemberOptionsResponse, error) {
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "compare loyalty member options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "country_code", req.GetDetails().GetDestination().GetCountryCode())
	calculateFuelCardRequest := &fuelv1.CalculateFuelCardRequest{
		Context: req.GetContext(),
		Details: &fuelv1.CalculateFuelCardRequest_Details{
			Destination: &commonv1.PostalAddress{CountryCode: tenancy.Value(ctx, "country_code")},
		},
		FuelCard: &fuelv1.FuelCard{
			Funding: &fuelv1.FuelCard_Bank{Bank: &fuelv1.FuelCardBankFunding{
				AccountHolder: req.GetLoyaltyMember().GetBank().GetAccountHolder(),
				Iban:          req.GetLoyaltyMember().GetBank().GetIban(),
			}},
			Contact: &commonv1.ContactInfo{Name: req.GetLoyaltyMember().GetContact().GetName()},
		},
	}
	calculateFuelCardReply, err := grpcx.Call(ctx, s.fuelCardCalculationClient.CalculateFuelCard, calculateFuelCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fuel.v1.FuelCardCalculationService/CalculateFuelCard")
	}
	lintMetricSeriesesRequest := &metricsv1.LintMetricSeriesesRequest{
		Context: req.GetContext(),
		MetricSeries: &metricsv1.MetricSeries{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetLoyaltyMember().GetAudit().GetCreatedBy()},
			Owner: &identityv1.UserRef{Email: req.GetLoyaltyMember().GetContact().GetEmail()},
		},
		Details: &metricsv1.LintMetricSeriesesRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetLoyaltyMember().GetContact().GetPreferredChannel(),
			},
		},
	}
	lintMetricSeriesesReply := new(metricsv1.LintMetricSeriesesResponse)
	if err := grpcx.Invoke(ctx, s.conn, metricsv1.MetricSeriesValidationService_LintMetricSerieses_FullMethodName, lintMetricSeriesesRequest, lintMetricSeriesesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesValidationService/LintMetricSerieses")
	}
	estimateSellerPayoutScheduleRequest := &sellersv1.EstimateSellerPayoutScheduleRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					Name:             calculateFuelCardReply.GetFuelCard().GetContact().GetName(),
					PreferredChannel: calculateFuelCardReply.GetFuelCard().GetContact().GetPreferredChannel(),
				},
			}},
			Location: &commonv1.PostalAddress{
				Region: lintMetricSeriesesReply.GetMetricSeries().GetAddress().GetRegion(),
			},
		},
	}
	type estimateSellerPayoutScheduleResult struct {
		reply *sellersv1.EstimateSellerPayoutScheduleResponse
		err   error
	}
	estimateSellerPayoutScheduleResults := make(chan estimateSellerPayoutScheduleResult, 1)
	go func() {
		reply, err := s.sellerPayoutScheduleCalculationClient.EstimateSellerPayoutSchedule(ctx, estimateSellerPayoutScheduleRequest)
		estimateSellerPayoutScheduleResults <- estimateSellerPayoutScheduleResult{reply, err}
	}()
	var estimateSellerPayoutScheduleReply *sellersv1.EstimateSellerPayoutScheduleResponse
	select {
	case result := <-estimateSellerPayoutScheduleResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/EstimateSellerPayoutSchedule")
		}
		estimateSellerPayoutScheduleReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &loyaltyv1.CompareLoyaltyMemberOptionsResponse{
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Available: calculateFuelCardReply.GetFuelCard().GetAvailable(),
			Audit: &commonv1.AuditInfo{
				UpdatedAt: lintMetricSeriesesReply.GetMetricSeries().GetAudit().GetUpdatedAt(),
				UpdatedBy: lintMetricSeriesesReply.GetMetricSeries().GetAudit().GetUpdatedBy(),
				CreatedAt: estimateSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetAudit().GetCreatedAt(),
			},
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
			CurrencyCode:    req.GetDetails().GetCurrencyCode(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecalculateLoyaltyMembers recalculates a loyalty member.
func (s *LoyaltyMemberCalculationServer) RecalculateLoyaltyMembers(ctx context.Context, req *loyaltyv1.RecalculateLoyaltyMembersRequest) (*loyaltyv1.RecalculateLoyaltyMembersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecalculateLoyaltyMembers", "error", err)
		return nil, err
	}
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "recalculate loyalty members", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	labels := make(map[string]string, len(req.GetLoyaltyMember().GetLabels()))
	for key, value := range req.GetLoyaltyMember().GetLabels() {
		labels[key] = value
	}
	listSynonymExportsRequest := &searchv1.ListSynonymExportsRequest{
		Context: req.GetContext(),
		Filter:  &searchv1.ListSynonymExportsRequest_Filter{Labels: labels},
	}
	listSynonymExportsReply, err := s.listSynonymExportsFn(ctx, listSynonymExportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/ListSynonymExports")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key2, value2 := range req.GetAnnotations() {
		annotations[key2] = value2
	}
	summarizeReviewModerationsRequest := &reviewsv1.SummarizeReviewModerationsRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      req.GetReason(),
		Details:     &reviewsv1.SummarizeReviewModerationsRequest_Details{Filters: listSynonymExportsReply.GetResult().GetDetails()},
	}
	summarizeReviewModerationsReply, err := grpcx.Call(ctx, s.reviewModerationQueryClient.SummarizeReviewModerations, summarizeReviewModerationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationQueryService/SummarizeReviewModerations")
	}
	return &loyaltyv1.RecalculateLoyaltyMembersResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: listSynonymExportsReply.GetResult().GetCorrelationId(),
			Details:       summarizeReviewModerationsReply.GetResult().GetDetails(),
			Message:       summarizeReviewModerationsReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
			Status:          req.GetLoyaltyMember().GetStatus(),
		},
	}, nil
}

// SimulateLoyaltyMember simulates a loyalty member.
func (s *LoyaltyMemberCalculationServer) SimulateLoyaltyMember(ctx context.Context, req *loyaltyv1.SimulateLoyaltyMemberRequest) (*loyaltyv1.SimulateLoyaltyMemberResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "simulate loyalty member", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetLoyaltyMember())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded loyaltyv1.LoyaltyMember
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	shareSearchIndexExportRequest := &searchv1.ShareSearchIndexExportRequest{
		Context: req.GetContext(),
		Details: &searchv1.ShareSearchIndexExportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				PreferredChannel: req.GetLoyaltyMember().GetContact().GetPreferredChannel(),
				Email:            (&decoded).GetContact().GetEmail(),
			},
		},
	}
	shareSearchIndexExportReply := new(searchv1.ShareSearchIndexExportResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", searchSearchIndexExportServiceName, "ShareSearchIndexExport"), shareSearchIndexExportRequest, shareSearchIndexExportReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/ShareSearchIndexExport")
	}
	return &loyaltyv1.SimulateLoyaltyMemberResponse{
		Result: &commonv1.OperationResult{
			Message: shareSearchIndexExportReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Audit: &commonv1.AuditInfo{
				Etag: shareSearchIndexExportReply.GetSearchIndex().GetAudit().GetEtag(),
			},
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
			Available:       req.GetLoyaltyMember().GetAvailable(),
		},
	}, nil
}

// ExplainLoyaltyMemberCalculation explains a loyalty member.
func (s *LoyaltyMemberCalculationServer) ExplainLoyaltyMemberCalculation(ctx context.Context, req *loyaltyv1.ExplainLoyaltyMemberCalculationRequest) (*loyaltyv1.ExplainLoyaltyMemberCalculationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "explain loyalty member calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	var accountHolder string
	switch choice := req.GetLoyaltyMember().GetFunding().(type) {
	case *loyaltyv1.LoyaltyMember_Card:
		accountHolder = choice.Card.GetHolderName()
	case *loyaltyv1.LoyaltyMember_Bank:
		accountHolder = choice.Bank.GetAccountHolder()
	case *loyaltyv1.LoyaltyMember_ExternalAccountId:
		accountHolder = choice.ExternalAccountId
	}
	estimateGiftCardBatchReply, err := s.giftCardBatchCalculationClient.EstimateGiftCardBatch(ctx, &giftcardsv1.EstimateGiftCardBatchRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit:      &commonv1.AuditInfo{UpdatedBy: req.GetLoyaltyMember().GetAudit().GetUpdatedBy()},
			Settlement: &giftcardsv1.GiftCardBatch_Bank{Bank: &giftcardsv1.GiftCardBatchBankSettlement{AccountHolder: accountHolder}},
			Counterparty: &giftcardsv1.GiftCardRef{
				DisplayName: req.GetLoyaltyMember().GetHolder().GetDisplayName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchCalculationService/EstimateGiftCardBatch")
	}
	packed, err := anypb.New(req.GetLoyaltyMember())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	createAuditEventRequest := &auditv1.CreateAuditEventRequest{
		Context: req.GetContext(),
		AuditEvent: &auditv1.AuditEvent{
			Subject: &auditv1.AuditTrailRef{
				Number: estimateGiftCardBatchReply.GetGiftCardBatch().GetNumber(),
			},
			Detail: packed,
		},
	}
	createAuditEventReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*auditv1.CreateAuditEventResponse, error) {
		return s.auditEventClient.CreateAuditEvent(ctx, createAuditEventRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventService/CreateAuditEvent")
	}
	return &loyaltyv1.ExplainLoyaltyMemberCalculationResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: estimateGiftCardBatchReply.GetResult().GetReferenceIds(),
			Details:      createAuditEventReply.GetResult().GetDetails(),
			Message:      createAuditEventReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: estimateGiftCardBatchReply.GetGiftCardBatch().GetAudit().GetUpdatedAt(),
			},
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
			Balance:         req.GetLoyaltyMember().GetBalance(),
		},
	}, nil
}

// GetLoyaltyMemberRates returns a loyalty member.
func (s *LoyaltyMemberCalculationServer) GetLoyaltyMemberRates(ctx context.Context, req *loyaltyv1.GetLoyaltyMemberRatesRequest) (*loyaltyv1.GetLoyaltyMemberRatesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "loyalty_member", req.GetLoyaltyMemberId(), "get_loyalty_member_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get loyalty member rates", "tenant", req.GetContext().GetTenantId())
	compareSellerListingOptionsReply, err := s.sellerListingCalculationClient.CompareSellerListingOptions(ctx, &sellersv1.CompareSellerListingOptionsRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingCalculationService/CompareSellerListingOptions")
	}
	validateSearchIndexRequest := &searchv1.ValidateSearchIndexRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Components: map[string]*searchv1.SearchIndexComponent{"primary": &searchv1.SearchIndexComponent{
				Description: compareSellerListingOptionsReply.GetSellerListing().GetComponents()["primary"].GetDescription(),
			}},
			Dimensions: &searchv1.SearchIndexDimensions{
				Unit: compareSellerListingOptionsReply.GetSellerListing().GetDimensions().GetUnit(),
			},
			Locator: &searchv1.SearchIndex_Address{Address: &commonv1.PostalAddress{
				Lines: compareSellerListingOptionsReply.GetSellerListing().GetAddress().GetLines(),
			}},
		},
	}
	send := s.validateSearchIndexSender(validateSearchIndexRequest)
	validateSearchIndexReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexValidationService/ValidateSearchIndex")
	}
	return &loyaltyv1.GetLoyaltyMemberRatesResponse{
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Audit: &commonv1.AuditInfo{
				CreatedBy: compareSellerListingOptionsReply.GetSellerListing().GetAudit().GetCreatedBy(),
			},
			Holder: &loyaltyv1.RewardRef{
				DisplayName: validateSearchIndexReply.GetSearchIndex().GetOwner().GetDisplayName(),
			},
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// validateSearchIndexSender returns a function that sends one request to
// acme.search.v1.SearchIndexValidationService/ValidateSearchIndex.
func (s *LoyaltyMemberCalculationServer) validateSearchIndexSender(request *searchv1.ValidateSearchIndexRequest) func(context.Context) (*searchv1.ValidateSearchIndexResponse, error) {
	return func(ctx context.Context) (*searchv1.ValidateSearchIndexResponse, error) {
		return s.searchIndexValidationClient.ValidateSearchIndex(ctx, request)
	}
}
