package promotions

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	seatingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/seating/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// RedemptionApprovalServer implements `acme.promotions.v1.RedemptionApprovalService`.
// Routes redemptions through approval, with escalation and delegation.
type RedemptionApprovalServer struct {
	promotionsv1.UnimplementedRedemptionApprovalServiceServer
	store                             *Store
	log                               *slog.Logger
	campaignSyncClient                promotionsv1.CampaignSyncServiceClient
	couponReportingClient             promotionsv1.CouponReportingServiceClient
	campaignApprovalClient            promotionsv1.CampaignApprovalServiceClient
	tenantExportClient                identityv1.TenantExportServiceClient
	tenantExportClientFallback        identityv1.TenantExportServiceClient
	bus                               *eventbus.Bus
	giftCardTransactionApprovalClient giftcardsv1.GiftCardTransactionApprovalServiceClient
	redemptionLifecycleClient         promotionsv1.RedemptionLifecycleServiceClient
	couponApprovalClient              promotionsv1.CouponApprovalServiceClient
	promotionApprovalClient           promotionsv1.PromotionApprovalServiceClient
	rewardLifecyclePort               rewardLifecyclePort
	meteredSearchIndexQuery           meteredSearchIndexQuery
	conn                              *grpc.ClientConn
	seatMapCalculationOnce            sync.Once
	seatMapCalculationCached          seatingv1.SeatMapCalculationServiceClient
	promotionWorkflowClient           promotionsv1.PromotionWorkflowServiceClient
	searchQueryExportClient           searchv1.SearchQueryExportServiceClient
}

// NewRedemptionApprovalServer wires a RedemptionApprovalServer.
func NewRedemptionApprovalServer(deps platform.Deps) *RedemptionApprovalServer {
	return &RedemptionApprovalServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.promotions.v1.RedemptionApprovalService"),
		campaignSyncClient:                promotionsv1.NewCampaignSyncServiceClient(deps.Conn),
		couponReportingClient:             promotionsv1.NewCouponReportingServiceClient(deps.Conn),
		campaignApprovalClient:            promotionsv1.NewCampaignApprovalServiceClient(deps.Conn),
		tenantExportClient:                identityv1.NewTenantExportServiceClient(deps.Conn),
		tenantExportClientFallback:        identityv1.NewTenantExportServiceClient(deps.Conn),
		bus:                               deps.Bus,
		giftCardTransactionApprovalClient: giftcardsv1.NewGiftCardTransactionApprovalServiceClient(deps.Conn),
		redemptionLifecycleClient:         promotionsv1.NewRedemptionLifecycleServiceClient(deps.Conn),
		couponApprovalClient:              promotionsv1.NewCouponApprovalServiceClient(deps.Conn),
		promotionApprovalClient:           promotionsv1.NewPromotionApprovalServiceClient(deps.Conn),
		rewardLifecyclePort:               newRewardLifecyclePort(deps),
		meteredSearchIndexQuery:           meteredSearchIndexQuery{SearchIndexQueryServiceClient: searchv1.NewSearchIndexQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		conn:                              deps.Conn,
		promotionWorkflowClient:           promotionsv1.NewPromotionWorkflowServiceClient(deps.Conn),
		searchQueryExportClient:           searchv1.NewSearchQueryExportServiceClient(deps.Conn),
	}
}

// SubmitRedemption submits a redemption.
func (s *RedemptionApprovalServer) SubmitRedemption(ctx context.Context, req *promotionsv1.SubmitRedemptionRequest) (*promotionsv1.SubmitRedemptionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubmitRedemption", "error", err)
		return nil, err
	}
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "submit redemption", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*promotionsv1.SubmitRedemptionRequest_Details)
	snapshot, err := protojson.Marshal(req.GetRedemption())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pushCampaignRequest := &promotionsv1.PushCampaignRequest{}
	pushCampaignRequest.Context = req.GetContext()
	pushCampaignRequest.Campaign = &promotionsv1.Campaign{
		Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		}},
		Organizer: &identityv1.UserRef{
			UserId:      req.GetDetails().GetApprover().GetUserId(),
			DisplayName: req.GetRedemption().GetSubject().GetDisplayName(),
			Email:       cloned.GetApproverEmail(),
		},
	}
	pushCampaignRequest.Details = &promotionsv1.PushCampaignRequest_Details{Payload: snapshot}
	pushCampaignReply, err := s.campaignSyncClient.PushCampaign(ctx, pushCampaignRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignSyncService/PushCampaign")
	}
	var envelope promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	streamCouponMetricsStream, err := s.couponReportingClient.StreamCouponMetrics(ctx, &promotionsv1.StreamCouponMetricsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		Filter: &promotionsv1.StreamCouponMetricsRequest_Filter{
			Region: pushCampaignReply.GetCampaign().GetLocation().GetRegion(),
			Query:  envelope.GetDisplayName(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponReportingService/StreamCouponMetrics")
	}
	for {
		received, err := streamCouponMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponReportingService/StreamCouponMetrics")
		}
		_ = received
	}
	return &promotionsv1.SubmitRedemptionResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: pushCampaignReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &promotionsv1.SubmitRedemptionResponse_Outcome{Status: pushCampaignReply.GetResult().GetStatus()},
		Redemption: &promotionsv1.Redemption{
			RedemptionId:  req.GetRedemptionId(),
			CorrelationId: req.GetRedemption().GetCorrelationId(),
		},
	}, nil
}

// ApproveRedemption approves a redemption.
func (s *RedemptionApprovalServer) ApproveRedemption(ctx context.Context, req *promotionsv1.ApproveRedemptionRequest) (*promotionsv1.ApproveRedemptionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ApproveRedemption", "error", err)
		return nil, err
	}
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "approve redemption", "tenant", req.GetContext().GetTenantId())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := redemptionApprovalServerApproveCampaignSteps[mode]
	var envelope promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	approveCampaignRequest := &promotionsv1.ApproveCampaignRequest{
		Context: req.GetContext(),
		Campaign: &promotionsv1.Campaign{
			Organizer: &identityv1.UserRef{
				DisplayName: req.GetRedemption().GetSubject().GetDisplayName(),
			},
			Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
			}},
		},
		Details:    &promotionsv1.ApproveCampaignRequest_Details{Conditions: req.GetDetails().GetConditions()},
		CampaignId: envelope.GetDisplayName(),
	}
	approveCampaignReply, err := run(s, ctx, approveCampaignRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignApprovalService/ApproveCampaign")
	}
	return &promotionsv1.ApproveRedemptionResponse{
		Result: &commonv1.OperationResult{
			Message: approveCampaignReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Redemption: &promotionsv1.Redemption{
			RedemptionId: req.GetRedemptionId(),
			Kind:         req.GetRedemption().GetKind(),
		},
	}, nil
}

// RejectRedemption rejects a redemption.
func (s *RedemptionApprovalServer) RejectRedemption(ctx context.Context, req *promotionsv1.RejectRedemptionRequest) (*promotionsv1.RejectRedemptionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "reject redemption", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *identityv1.CancelTenantExportRequest, ...grpc.CallOption) (*identityv1.CancelTenantExportResponse, error){
		"primary":  s.tenantExportClient.CancelTenantExport,
		"fallback": s.tenantExportClientFallback.CancelTenantExport,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	cloned := proto.Clone(req.GetDetails()).(*promotionsv1.RejectRedemptionRequest_Details)
	var holder struct{ DisplayName string }
	if record := req.GetDetails().GetApprover(); record != nil {
		if err := mapper.CopyFields(&holder, record, "DisplayName"); err != nil {
			s.log.WarnContext(ctx, "copy display_name", "error", err)
		}
	}
	cancelTenantExportRequest := &identityv1.CancelTenantExportRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Details: &identityv1.CancelTenantExportRequest_Details{
			NotifyContact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
			NewOwner: &identityv1.UserRef{
				UserId:      cloned.GetApprover().GetUserId(),
				DisplayName: holder.DisplayName,
			},
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	}
	cancelTenantExportReply, err := route(ctx, cancelTenantExportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/CancelTenantExport")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "promotions.reject_redemption.list_order_workflows",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"filter.region":     cancelTenantExportReply.GetTenant().GetLocation().GetRegion(),
			"filter.owner_id":   req.GetRedemption().GetSubject().GetCouponId(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &promotionsv1.RejectRedemptionResponse{
		Outcome: &promotionsv1.RejectRedemptionResponse_Outcome{
			ReferenceId: cancelTenantExportReply.GetOutcome().GetReferenceId(),
		},
		Redemption: &promotionsv1.Redemption{
			RedemptionId: req.GetRedemptionId(),
			Sequence:     req.GetRedemption().GetSequence(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// DelegateRedemptionApproval delegates a redemption.
func (s *RedemptionApprovalServer) DelegateRedemptionApproval(ctx context.Context, req *promotionsv1.DelegateRedemptionApprovalRequest) (*promotionsv1.DelegateRedemptionApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "delegate redemption approval", "tenant", req.GetContext().GetTenantId())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*giftcardsv1.ApproveGiftCardTransactionResponse, len(req.GetRedemption().GetMeasurements()))
	for i, measurement := range req.GetRedemption().GetMeasurements() {
		group.Go(func() error {
			reply, err := s.giftCardTransactionApprovalClient.ApproveGiftCardTransaction(groupCtx, &giftcardsv1.ApproveGiftCardTransactionRequest{
				Context: req.GetContext(),
				GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
					Measurements: []*giftcardsv1.GiftCardTransactionMeasurement{&giftcardsv1.GiftCardTransactionMeasurement{
						Unit:       measurement.GetUnit(),
						ObservedAt: measurement.GetObservedAt(),
						Value:      measurement.GetValue(),
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
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionApprovalService/ApproveGiftCardTransaction")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	packed, err := anypb.New(req.GetRedemption())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	submitGiftCardTransactionRequest := &giftcardsv1.SubmitGiftCardTransactionRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: req.GetRedemption().GetSubject().GetDisplayName(),
			},
			Detail: packed,
		},
		GiftCardTransactionId: envelope.GetDisplayName(),
	}
	submitGiftCardTransactionReply, err := submitGiftCardTransactionCall(ctx, submitGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionApprovalService/SubmitGiftCardTransaction")
	}
	group2, groupCtx2 := errgroup.WithContext(ctx)
	group2.SetLimit(8)
	replies2 := make([]*promotionsv1.ActivateRedemptionResponse, len(req.GetRedemption().GetMeasurements()))
	for i2, measurement2 := range req.GetRedemption().GetMeasurements() {
		group2.Go(func() error {
			reply, err := s.redemptionLifecycleClient.ActivateRedemption(groupCtx2, &promotionsv1.ActivateRedemptionRequest{
				Context:      req.GetContext(),
				RedemptionId: req.GetRedemptionId(),
				Details: &promotionsv1.ActivateRedemptionRequest_Details{
					NewOwner: &identityv1.UserRef{
						DisplayName: req.GetRedemption().GetSubject().GetDisplayName(),
						Email:       req.GetDetails().GetApproverEmail(),
					},
					EffectiveAt:   measurement2.GetObservedAt(),
					NotifyContact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
				},
			})
			if err != nil {
				return err
			}
			replies2[i2] = reply
			return nil
		})
	}
	if err := group2.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionLifecycleService/ActivateRedemption")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies2))
	jobs := make(chan *promotionsv1.RedemptionMeasurement)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for measurement3 := range jobs {
				if _, err := s.couponApprovalClient.RecallCoupon(workCtx, &promotionsv1.RecallCouponRequest{
					Context: req.GetContext(),
					Details: &promotionsv1.RecallCouponRequest_Details{
						Approver: &identityv1.UserRef{
							DisplayName: submitGiftCardTransactionReply.GetGiftCardTransaction().GetSubject().GetDisplayName(),
						},
					},
					Coupon: &promotionsv1.Coupon{
						Audit: &commonv1.AuditInfo{CreatedAt: measurement3.GetObservedAt()},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetRedemption().GetMeasurements() {
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
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponApprovalService/RecallCoupon")
	}
	return &promotionsv1.DelegateRedemptionApprovalResponse{
		Outcome: &promotionsv1.DelegateRedemptionApprovalResponse_Outcome{
			NextSteps: submitGiftCardTransactionReply.GetOutcome().GetNextSteps(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: submitGiftCardTransactionReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Redemption: &promotionsv1.Redemption{
			RedemptionId: req.GetRedemptionId(),
			Attributes:   req.GetRedemption().GetAttributes(),
		},
	}, nil
}

// RecallRedemption recalls a redemption.
func (s *RedemptionApprovalServer) RecallRedemption(ctx context.Context, req *promotionsv1.RecallRedemptionRequest) (*promotionsv1.RecallRedemptionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "recall redemption", "tenant", req.GetContext().GetTenantId())
	recallPromotionReply, err := s.promotionApprovalClient.RecallPromotion(ctx, &promotionsv1.RecallPromotionRequest{
		Context: req.GetContext(),
		Promotion: &promotionsv1.Promotion{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetRedemption().GetSubject().GetDisplayName(),
			},
		},
		Details: &promotionsv1.RecallPromotionRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionApprovalService/RecallPromotion")
	}
	var envelope promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	scheduleRewardExpiryRequest := &loyaltyv1.ScheduleRewardExpiryRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Audit: &commonv1.AuditInfo{
				CreatedBy: recallPromotionReply.GetPromotion().GetAudit().GetCreatedBy(),
			},
		},
		RewardId: envelope.GetDisplayName(),
	}
	scheduleRewardExpiryReply, err := s.rewardLifecyclePort.ScheduleRewardExpiry(ctx, scheduleRewardExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardLifecycleService/ScheduleRewardExpiry")
	}
	return &promotionsv1.RecallRedemptionResponse{
		Outcome: &promotionsv1.RecallRedemptionResponse_Outcome{
			Message: recallPromotionReply.GetResult().GetMessage(),
			Status:  scheduleRewardExpiryReply.GetOutcome().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Message: recallPromotionReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Redemption: &promotionsv1.Redemption{
			RedemptionId: req.GetRedemptionId(),
			Kind:         req.GetRedemption().GetKind(),
		},
	}, nil
}

// CommentRedemptionApproval comments a redemption.
func (s *RedemptionApprovalServer) CommentRedemptionApproval(ctx context.Context, req *promotionsv1.CommentRedemptionApprovalRequest) (*promotionsv1.CommentRedemptionApprovalResponse, error) {
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "comment redemption approval", "tenant", req.GetContext().GetTenantId())
	var envelope promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	summarizeSearchIndexesRequest := &searchv1.SummarizeSearchIndexesRequest{
		Context:       req.GetContext(),
		Annotations:   req.GetAnnotations(),
		SearchIndexId: envelope.GetDisplayName(),
	}
	summarizeSearchIndexesReply, err := s.meteredSearchIndexQuery.SummarizeSearchIndexes(ctx, summarizeSearchIndexesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexQueryService/SummarizeSearchIndexes")
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded promotionsv1.CommentRedemptionApprovalRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	var envelope2 promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	estimateSeatMapRequest := &seatingv1.EstimateSeatMapRequest{
		Context: req.GetContext(),
		SeatMap: &seatingv1.SeatMap{
			Children: []*seatingv1.SeatMap{&seatingv1.SeatMap{
				Children: []*seatingv1.SeatMap{&seatingv1.SeatMap{
					Manager: &identityv1.UserRef{
						DisplayName: req.GetRedemption().GetSubject().GetDisplayName(),
						UserId:      req.GetDetails().GetApprover().GetUserId(),
						Email:       (&decoded).GetApprover().GetEmail(),
					},
					Attributes: req.GetRedemption().GetAttributes(),
				}},
			}},
		},
		SeatMapId: envelope2.GetDisplayName(),
	}
	estimateSeatMapReply, err := s.seatMapCalculationLazy().EstimateSeatMap(ctx, estimateSeatMapRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.seating.v1.SeatMapCalculationService/EstimateSeatMap")
	}
	advancePromotionWorkflowReply, err := s.promotionWorkflowClient.AdvancePromotionWorkflow(ctx, &promotionsv1.AdvancePromotionWorkflowRequest{
		Context: req.GetContext(),
		Promotion: &promotionsv1.Promotion{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetRedemption().GetSubject().GetDisplayName(),
			},
		},
		Details: &promotionsv1.AdvancePromotionWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionWorkflowService/AdvancePromotionWorkflow")
	}
	attributes := make(map[string]string, len(req.GetRedemption().GetAttributes()))
	for key, value := range req.GetRedemption().GetAttributes() {
		attributes[key] = value
	}
	var envelope3 promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope3); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	step := &redemptionApprovalServerCommentRedemptionApprovalScheduleSearchQueryExportStep{request: &searchv1.ScheduleSearchQueryExportRequest{
		Context:     req.GetContext(),
		Reason:      advancePromotionWorkflowReply.GetPromotion().GetBlock().GetReason(),
		Annotations: req.GetAnnotations(),
		Details: &searchv1.ScheduleSearchQueryExportRequest_Details{
			Period:  summarizeSearchIndexesReply.GetPeriod(),
			Filters: attributes,
		},
		SearchQueryId: envelope3.GetDisplayName(),
	}}
	if err := s.runCommentRedemptionApprovalScheduleSearchQueryExport(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryExportService/ScheduleSearchQueryExport")
	}
	scheduleSearchQueryExportReply := step.reply
	_ = scheduleSearchQueryExportReply
	return &promotionsv1.CommentRedemptionApprovalResponse{
		Redemption: &promotionsv1.Redemption{
			CorrelationId: summarizeSearchIndexesReply.GetResult().GetCorrelationId(),
			RedemptionId:  req.GetRedemptionId(),
		},
		Outcome: &promotionsv1.CommentRedemptionApprovalResponse_Outcome{
			Message: summarizeSearchIndexesReply.GetResult().GetMessage(),
			Status:  estimateSeatMapReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds:  estimateSeatMapReply.GetResult().GetReferenceIds(),
			Status:        advancePromotionWorkflowReply.GetResult().GetStatus(),
			CorrelationId: req.GetRedemption().GetCorrelationId(),
		},
	}, nil
}

// redemptionApprovalServerApproveCampaignSteps are the ways to run
// acme.promotions.v1.CampaignApprovalService/ApproveCampaign, by name.
var redemptionApprovalServerApproveCampaignSteps = map[string]func(*RedemptionApprovalServer, context.Context, *promotionsv1.ApproveCampaignRequest) (*promotionsv1.ApproveCampaignResponse, error){
	"default": (*RedemptionApprovalServer).approveCampaignStep,
	"audited": (*RedemptionApprovalServer).approveCampaignAuditedStep,
}

func (s *RedemptionApprovalServer) approveCampaignStep(ctx context.Context, request *promotionsv1.ApproveCampaignRequest) (*promotionsv1.ApproveCampaignResponse, error) {
	return s.campaignApprovalClient.ApproveCampaign(ctx, request)
}

func (s *RedemptionApprovalServer) approveCampaignAuditedStep(ctx context.Context, request *promotionsv1.ApproveCampaignRequest) (*promotionsv1.ApproveCampaignResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.promotions.v1.CampaignApprovalService/ApproveCampaign")
	return s.campaignApprovalClient.ApproveCampaign(ctx, request)
}

// seatMapCalculationLazy dials acme.seating.v1.SeatMapCalculationService the first time a handler needs it.
func (s *RedemptionApprovalServer) seatMapCalculationLazy() seatingv1.SeatMapCalculationServiceClient {
	s.seatMapCalculationOnce.Do(func() { s.seatMapCalculationCached = seatingv1.NewSeatMapCalculationServiceClient(s.conn) })
	return s.seatMapCalculationCached
}

// redemptionApprovalServerCommentRedemptionApprovalScheduleSearchQueryExportStep carries the request of one step of
// CommentRedemptionApproval and, once it ran, its reply.
type redemptionApprovalServerCommentRedemptionApprovalScheduleSearchQueryExportStep struct {
	request *searchv1.ScheduleSearchQueryExportRequest
	reply   *searchv1.ScheduleSearchQueryExportResponse
}

func (s *RedemptionApprovalServer) runCommentRedemptionApprovalScheduleSearchQueryExport(ctx context.Context, step *redemptionApprovalServerCommentRedemptionApprovalScheduleSearchQueryExportStep) error {
	reply, err := s.searchQueryExportClient.ScheduleSearchQueryExport(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
