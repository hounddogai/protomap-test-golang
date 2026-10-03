package promotions

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// RedemptionImportServer implements `acme.promotions.v1.RedemptionImportService`.
// Takes redemptions in bulk from partners and legacy systems.
type RedemptionImportServer struct {
	promotionsv1.UnimplementedRedemptionImportServiceServer
	store                    *Store
	log                      *slog.Logger
	couponValidationClient   promotionsv1.CouponValidationServiceClient
	searchIndexClient        searchv1.SearchIndexServiceClient
	promotionReportingClient promotionsv1.PromotionReportingServiceClient
	orderNoteSyncClient      ordersv1.OrderNoteSyncServiceClient
	userClient               identityv1.UserServiceClient
	summarizeSearchQueriesFn func(context.Context, *searchv1.SummarizeSearchQueriesRequest, ...grpc.CallOption) (*searchv1.SummarizeSearchQueriesResponse, error)
	userValidationClient     identityv1.UserValidationServiceClient
	clients                  *registry.Registry
	metricSeriesClient       metricsv1.MetricSeriesServiceClient
	couponValidationPort     couponValidationPort
}

// NewRedemptionImportServer wires a RedemptionImportServer.
func NewRedemptionImportServer(deps platform.Deps) *RedemptionImportServer {
	searchQueryQuery := searchv1.NewSearchQueryQueryServiceClient(deps.Conn)
	return &RedemptionImportServer{
		store:                    NewStore(deps.DB),
		log:                      deps.Logger.With("service", "acme.promotions.v1.RedemptionImportService"),
		couponValidationClient:   promotionsv1.NewCouponValidationServiceClient(deps.Conn),
		searchIndexClient:        searchv1.NewSearchIndexServiceClient(deps.Conn),
		promotionReportingClient: promotionsv1.NewPromotionReportingServiceClient(deps.Conn),
		orderNoteSyncClient:      ordersv1.NewOrderNoteSyncServiceClient(deps.Conn),
		userClient:               identityv1.NewUserServiceClient(deps.Conn),
		summarizeSearchQueriesFn: searchQueryQuery.SummarizeSearchQueries,
		userValidationClient:     identityv1.NewUserValidationServiceClient(deps.Conn),
		clients:                  deps.Clients,
		metricSeriesClient:       metricsv1.NewMetricSeriesServiceClient(deps.Conn),
		couponValidationPort:     newCouponValidationPort(deps),
	}
}

// ImportRedemptions takes a stream of redemptions and answers once at the end.
func (s *RedemptionImportServer) ImportRedemptions(stream grpc.ClientStreamingServer[promotionsv1.ImportRedemptionsRequest, promotionsv1.ImportRedemptionsResponse]) error {
	ctx := stream.Context()
	var accepted int64
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		s.log.InfoContext(ctx, "import redemptions", "tenant", msg.GetContext().GetTenantId())
		scoreCouponRequest := &promotionsv1.ScoreCouponRequest{}
		scoreCouponRequest.Context = msg.GetContext()
		scoreCouponRequest.Coupon = &promotionsv1.Coupon{
			Contact: &commonv1.ContactInfo{Name: msg.GetHeader().GetSubmittedBy().GetName()},
		}
		scoreCouponRequest.Details = &promotionsv1.ScoreCouponRequest_Details{
			CurrencyCode: msg.GetRecord().GetSubject().GetCurrencyCode(),
		}
		if _, err := s.couponValidationClient.ScoreCoupon(ctx, scoreCouponRequest); err != nil {
			return grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/ScoreCoupon")
		}
		for _, measurement := range msg.GetRecord().GetMeasurements() {
			if _, err := s.searchIndexClient.CreateSearchIndex(ctx, &searchv1.CreateSearchIndexRequest{
				Context: msg.GetContext(),
				SearchIndex: &searchv1.SearchIndex{
					Dimensions: &searchv1.SearchIndexDimensions{Unit: measurement.GetUnit()},
					Owner:      &identityv1.UserRef{DisplayName: msg.GetRecord().GetSubject().GetDisplayName()},
				},
			}); err != nil {
				return grpcx.Wrap(err, "acme.search.v1.SearchIndexService/CreateSearchIndex")
			}
		}
		schedulePromotionReportRequest := &promotionsv1.SchedulePromotionReportRequest{
			Context: msg.GetContext(),
			Details: &promotionsv1.SchedulePromotionReportRequest_Details{Format: msg.GetHeader().GetFormat()},
		}
		if _, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*promotionsv1.SchedulePromotionReportResponse, error) {
			return s.promotionReportingClient.SchedulePromotionReport(ctx, schedulePromotionReportRequest)
		}); err != nil {
			return grpcx.Wrap(err, "acme.promotions.v1.PromotionReportingService/SchedulePromotionReport")
		}
		accepted++
	}
	return stream.SendAndClose(&promotionsv1.ImportRedemptionsResponse{Accepted: accepted})
}

// GetRedemptionImportStatus returns a redemption.
func (s *RedemptionImportServer) GetRedemptionImportStatus(ctx context.Context, req *promotionsv1.GetRedemptionImportStatusRequest) (*promotionsv1.GetRedemptionImportStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetRedemptionImportStatus", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "redemption", req.GetRedemptionId(), "get_redemption_import_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get redemption import status", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	snapshot, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pushOrderNoteReply, err := s.orderNoteSyncClient.PushOrderNote(ctx, &ordersv1.PushOrderNoteRequest{
		Context: req.GetContext(),
		OrderNote: &ordersv1.OrderNote{
			OccurredAt: req.GetContext().GetDeadline(),
			Detail:     packed,
		},
		Details: &ordersv1.PushOrderNoteRequest_Details{Payload: snapshot},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteSyncService/PushOrderNote")
	}
	return &promotionsv1.GetRedemptionImportStatusResponse{
		Redemption: &promotionsv1.Redemption{
			CorrelationId: pushOrderNoteReply.GetResult().GetCorrelationId(),
			Sequence:      pushOrderNoteReply.GetOrderNote().GetSequence(),
			RedemptionId:  req.GetRedemptionId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CancelRedemptionImport cancels a redemption.
func (s *RedemptionImportServer) CancelRedemptionImport(ctx context.Context, req *promotionsv1.CancelRedemptionImportRequest) (*promotionsv1.CancelRedemptionImportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "redemption", req.GetRedemptionId(), "cancel_redemption_import"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "cancel redemption import", "tenant", req.GetContext().GetTenantId())
	getUserRequest := &identityv1.GetUserRequest{
		Context: req.GetContext(),
		UserId:  req.GetDetails().GetNewOwner().GetUserId(),
	}
	getUserReply, err := identityv1.UserServiceClient.GetUser(s.userClient, ctx, getUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/GetUser")
	}
	return &promotionsv1.CancelRedemptionImportResponse{
		Outcome: &promotionsv1.CancelRedemptionImportResponse_Outcome{
			Status:      getUserReply.GetResult().GetStatus(),
			Message:     getUserReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Redemption: &promotionsv1.Redemption{RedemptionId: req.GetRedemptionId()},
		Result:     &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ValidateRedemptionImport validates a redemption.
func (s *RedemptionImportServer) ValidateRedemptionImport(ctx context.Context, req *promotionsv1.ValidateRedemptionImportRequest) (*promotionsv1.ValidateRedemptionImportResponse, error) {
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "validate redemption import", "tenant", req.GetContext().GetTenantId())
	var envelope promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	summarizeSearchQueriesRequest := &searchv1.SummarizeSearchQueriesRequest{
		Context:       req.GetContext(),
		Reason:        req.GetReason(),
		SearchQueryId: envelope.GetDisplayName(),
	}
	summarizeSearchQueriesReply, err := s.summarizeSearchQueriesFn(ctx, summarizeSearchQueriesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/SummarizeSearchQueries")
	}
	var envelope2 promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	explainUserDecisionReply, err := s.userValidationClient.ExplainUserDecision(ctx, &identityv1.ExplainUserDecisionRequest{
		Context: req.GetContext(),
		Details: &identityv1.ExplainUserDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetContact().GetPhone()},
		},
		UserId: envelope2.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/ExplainUserDecision")
	}
	return &promotionsv1.ValidateRedemptionImportResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: summarizeSearchQueriesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &promotionsv1.ValidateRedemptionImportResponse_Outcome{
			Message: summarizeSearchQueriesReply.GetResult().GetMessage(),
		},
		Redemption: &promotionsv1.Redemption{
			Attributes:    explainUserDecisionReply.GetUser().GetAttributes(),
			RedemptionId:  req.GetRedemptionId(),
			CorrelationId: req.GetRedemption().GetCorrelationId(),
		},
	}, nil
}

// RetryRedemptionImport retrys a redemption.
func (s *RedemptionImportServer) RetryRedemptionImport(ctx context.Context, req *promotionsv1.RetryRedemptionImportRequest) (*promotionsv1.RetryRedemptionImportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RetryRedemptionImport", "error", err)
		return nil, err
	}
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "retry redemption import", "tenant", req.GetContext().GetTenantId())
	user, ok := s.clients.Get("acme.identity.v1.UserService").(identityv1.UserServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.identity.v1.UserService")
	}
	var envelope promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	cloneUserRequest := &identityv1.CloneUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNotify().GetEmail()},
			UserId:  envelope.GetDisplayName(),
		},
	}
	cloneUserReply, err := user.CloneUser(ctx, cloneUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/CloneUser")
	}
	var envelope2 promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	watchMetricSeriesesStream, err := s.metricSeriesClient.WatchMetricSerieses(ctx, &metricsv1.WatchMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter: &metricsv1.WatchMetricSeriesesRequest_Filter{
			Region: cloneUserReply.GetUser().GetPrimaryAddress().GetRegion(),
			Query:  envelope2.GetDisplayName(),
		},
		Subscriber: &commonv1.ContactInfo{Email: cloneUserReply.GetUser().GetContact().GetEmail()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesService/WatchMetricSerieses")
	}
	for {
		received, err := watchMetricSeriesesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesService/WatchMetricSerieses")
		}
		_ = received
	}
	return &promotionsv1.RetryRedemptionImportResponse{
		Redemption: &promotionsv1.Redemption{
			Attributes:   cloneUserReply.GetUser().GetAttributes(),
			RedemptionId: req.GetRedemptionId(),
			OccurredAt:   req.GetRedemption().GetOccurredAt(),
		},
		Outcome: &promotionsv1.RetryRedemptionImportResponse_Outcome{Message: cloneUserReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// MapRedemptionImportColumns maps a redemption.
func (s *RedemptionImportServer) MapRedemptionImportColumns(ctx context.Context, req *promotionsv1.MapRedemptionImportColumnsRequest) (*promotionsv1.MapRedemptionImportColumnsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "map redemption import columns", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetContact().GetEmail())
	var envelope promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	explainCouponDecisionRequest := &promotionsv1.ExplainCouponDecisionRequest{
		Context: req.GetContext(),
		Details: &promotionsv1.ExplainCouponDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
			Strict:  req.GetDetails().GetStrict(),
		},
		Coupon: &promotionsv1.Coupon{
			Holder: &promotionsv1.PromotionRef{
				DisplayName: req.GetRedemption().GetSubject().GetDisplayName(),
			},
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetContact().GetPhone()},
		},
		CouponId: envelope.GetDisplayName(),
	}
	explainCouponDecisionReply, err := s.couponValidationPort.ExplainCouponDecision(ctx, explainCouponDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/ExplainCouponDecision")
	}
	return &promotionsv1.MapRedemptionImportColumnsResponse{
		Redemption: &promotionsv1.Redemption{
			Subject: &promotionsv1.CouponRef{
				CouponId: explainCouponDecisionReply.GetCoupon().GetCouponId(),
			},
			RedemptionId: req.GetRedemptionId(),
			Kind:         req.GetRedemption().GetKind(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
