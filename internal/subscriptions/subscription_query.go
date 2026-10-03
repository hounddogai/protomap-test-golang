package subscriptions

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// SubscriptionQueryServer implements `acme.subscriptions.v1.SubscriptionQueryService`.
// Answers read-only questions about subscriptions, for screens and reports.
type SubscriptionQueryServer struct {
	subscriptionsv1.UnimplementedSubscriptionQueryServiceServer
	store                                   *Store
	log                                     *slog.Logger
	ledgerAccountClient                     ledgerv1.LedgerAccountServiceClient
	giftCardBatchCalculationClient          giftcardsv1.GiftCardBatchCalculationServiceClient
	meteredSubscriptionPlanValidation       meteredSubscriptionPlanValidation
	validateUsageRecordFn                   func(context.Context, *subscriptionsv1.ValidateUsageRecordRequest, ...grpc.CallOption) (*subscriptionsv1.ValidateUsageRecordResponse, error)
	conn                                    *grpc.ClientConn
	reviewVoteValidationClient              reviewsv1.ReviewVoteValidationServiceClient
	usageRecordValidationPort               usageRecordValidationPort
	userHistoryClient                       identityv1.UserHistoryServiceClient
	compareSearchQueryVersionsFn            func(context.Context, *searchv1.CompareSearchQueryVersionsRequest, ...grpc.CallOption) (*searchv1.CompareSearchQueryVersionsResponse, error)
	renewalScheduleValidationClient         subscriptionsv1.RenewalScheduleValidationServiceClient
	renewalScheduleValidationClientFallback subscriptionsv1.RenewalScheduleValidationServiceClient
}

// NewSubscriptionQueryServer wires a SubscriptionQueryServer.
func NewSubscriptionQueryServer(deps platform.Deps) *SubscriptionQueryServer {
	usageRecordValidation := subscriptionsv1.NewUsageRecordValidationServiceClient(deps.Conn)
	searchQueryHistory := searchv1.NewSearchQueryHistoryServiceClient(deps.Conn)
	return &SubscriptionQueryServer{
		store:                                   NewStore(deps.DB),
		log:                                     deps.Logger.With("service", "acme.subscriptions.v1.SubscriptionQueryService"),
		ledgerAccountClient:                     ledgerv1.NewLedgerAccountServiceClient(deps.Conn),
		giftCardBatchCalculationClient:          giftcardsv1.NewGiftCardBatchCalculationServiceClient(deps.Conn),
		meteredSubscriptionPlanValidation:       meteredSubscriptionPlanValidation{SubscriptionPlanValidationServiceClient: subscriptionsv1.NewSubscriptionPlanValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		validateUsageRecordFn:                   usageRecordValidation.ValidateUsageRecord,
		conn:                                    deps.Conn,
		reviewVoteValidationClient:              reviewsv1.NewReviewVoteValidationServiceClient(deps.Conn),
		usageRecordValidationPort:               newUsageRecordValidationPort(deps),
		userHistoryClient:                       identityv1.NewUserHistoryServiceClient(deps.Conn),
		compareSearchQueryVersionsFn:            searchQueryHistory.CompareSearchQueryVersions,
		renewalScheduleValidationClient:         subscriptionsv1.NewRenewalScheduleValidationServiceClient(deps.Conn),
		renewalScheduleValidationClientFallback: subscriptionsv1.NewRenewalScheduleValidationServiceClient(deps.Conn),
	}
}

// LookupSubscription returns a subscription.
func (s *SubscriptionQueryServer) LookupSubscription(ctx context.Context, req *subscriptionsv1.LookupSubscriptionRequest) (*subscriptionsv1.LookupSubscriptionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "subscription", req.GetSubscriptionId(), "lookup_subscription"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup subscription", "tenant", req.GetContext().GetTenantId())
	watchLedgerAccountsStream, err := s.ledgerAccountClient.WatchLedgerAccounts(ctx, &ledgerv1.WatchLedgerAccountsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/WatchLedgerAccounts")
	}
	for {
		received, err := watchLedgerAccountsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/WatchLedgerAccounts")
		}
		_ = received
	}
	return &subscriptionsv1.LookupSubscriptionResponse{
		Subscription: &subscriptionsv1.Subscription{SubscriptionId: req.GetSubscriptionId()},
		Result:       &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BrowseSubscriptions lists subscriptions a page at a time.
func (s *SubscriptionQueryServer) BrowseSubscriptions(ctx context.Context, req *subscriptionsv1.BrowseSubscriptionsRequest) (*subscriptionsv1.BrowseSubscriptionsResponse, error) {
	s.log.InfoContext(ctx, "browse subscriptions", "tenant", req.GetContext().GetTenantId())
	quoteGiftCardBatchRequest := buildQuoteGiftCardBatchRequestForBrowseSubscriptions(req)
	quoteGiftCardBatchReply, err := s.giftCardBatchCalculationClient.QuoteGiftCardBatch(ctx, quoteGiftCardBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchCalculationService/QuoteGiftCardBatch")
	}
	normalizeSubscriptionPlanRequest := &subscriptionsv1.NormalizeSubscriptionPlanRequest{
		Context: req.GetContext(),
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: quoteGiftCardBatchReply.GetGiftCardBatch().GetAudit().GetUpdatedBy(),
				CreatedBy: quoteGiftCardBatchReply.GetGiftCardBatch().GetAudit().GetCreatedBy(),
				CreatedAt: quoteGiftCardBatchReply.GetGiftCardBatch().GetAudit().GetCreatedAt(),
			},
		},
	}
	normalizeSubscriptionPlanReply, err := s.meteredSubscriptionPlanValidation.NormalizeSubscriptionPlan(ctx, normalizeSubscriptionPlanRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanValidationService/NormalizeSubscriptionPlan")
	}
	return &subscriptionsv1.BrowseSubscriptionsResponse{
		Subscriptions: []*subscriptionsv1.Subscription{&subscriptionsv1.Subscription{
			Holder: &customersv1.CustomerRef{
				DisplayName: quoteGiftCardBatchReply.GetGiftCardBatch().GetCounterparty().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: normalizeSubscriptionPlanReply.GetSubscriptionPlan().GetAudit().GetUpdatedBy(),
				CreatedBy: normalizeSubscriptionPlanReply.GetSubscriptionPlan().GetAudit().GetCreatedBy(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// FindSubscriptions lists subscriptions a page at a time.
func (s *SubscriptionQueryServer) FindSubscriptions(ctx context.Context, req *subscriptionsv1.FindSubscriptionsRequest) (*subscriptionsv1.FindSubscriptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected FindSubscriptions", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "find subscriptions", "tenant", req.GetContext().GetTenantId())
	source := req.GetContext().GetActor().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	validateUsageRecordRequest := &subscriptionsv1.ValidateUsageRecordRequest{
		Context: req.GetContext(),
		UsageRecord: &subscriptionsv1.UsageRecord{
			Subject:    &customersv1.CustomerRef{Email: email},
			OccurredAt: req.GetFilter().GetCreatedAfter(),
		},
	}
	validateUsageRecordReply, err := s.validateUsageRecordFn(ctx, validateUsageRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordValidationService/ValidateUsageRecord")
	}
	return &subscriptionsv1.FindSubscriptionsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: validateUsageRecordReply.GetResult().GetReferenceIds(),
			Message:      validateUsageRecordReply.GetOutcome().GetMessage(),
			Status:       "ok",
		},
	}, nil
}

// CountSubscriptions counts subscriptions by status.
func (s *SubscriptionQueryServer) CountSubscriptions(ctx context.Context, req *subscriptionsv1.CountSubscriptionsRequest) (*subscriptionsv1.CountSubscriptionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "count subscriptions", "tenant", req.GetContext().GetTenantId())
	filterCache := cache.New[string, *subscriptionsv1.CountSubscriptionsRequest_Filter](1)
	filterCache.Put(req.GetContext().GetTraceId(), req.GetFilter())
	cachedFilter, _ := filterCache.Get(req.GetContext().GetTraceId())
	scoreRenewalScheduleRequest := &subscriptionsv1.ScoreRenewalScheduleRequest{
		Context: req.GetContext(),
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			Participants: []*subscriptionsv1.RenewalScheduleParticipant{&subscriptionsv1.RenewalScheduleParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			}},
			Organizer: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Audit:     &commonv1.AuditInfo{UpdatedAt: cachedFilter.GetCreatedAfter()},
		},
	}
	scoreRenewalScheduleReply := new(subscriptionsv1.ScoreRenewalScheduleResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", subscriptionsRenewalScheduleValidationServiceName, "ScoreRenewalSchedule"), scoreRenewalScheduleRequest, scoreRenewalScheduleReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.RenewalScheduleValidationService/ScoreRenewalSchedule")
	}
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	validateReviewVoteRequest := &reviewsv1.ValidateReviewVoteRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.ValidateReviewVoteRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: scoreRenewalScheduleReply.GetRenewalSchedule().GetOrganizer().GetEmail(),
			},
		},
		ReviewVote: &reviewsv1.ReviewVote{
			CorrelationId: scoreRenewalScheduleReply.GetResult().GetCorrelationId(),
			Detail:        packed,
		},
	}
	validateReviewVoteReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*reviewsv1.ValidateReviewVoteResponse, error) {
		return s.reviewVoteValidationClient.ValidateReviewVote(ctx, validateReviewVoteRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteValidationService/ValidateReviewVote")
	}
	return &subscriptionsv1.CountSubscriptionsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: scoreRenewalScheduleReply.GetResult().GetCorrelationId(),
			Status:        scoreRenewalScheduleReply.GetResult().GetStatus(),
			Details:       validateReviewVoteReply.GetResult().GetDetails(),
		},
	}, nil
}

// ResolveSubscriptions returns several subscriptions at once.
func (s *SubscriptionQueryServer) ResolveSubscriptions(ctx context.Context, req *subscriptionsv1.ResolveSubscriptionsRequest) (*subscriptionsv1.ResolveSubscriptionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "resolve subscriptions", "tenant", req.GetContext().GetTenantId())
	return &subscriptionsv1.ResolveSubscriptionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ExportSubscriptions streams every matching subscription.
func (s *SubscriptionQueryServer) ExportSubscriptions(req *subscriptionsv1.ExportSubscriptionsRequest, stream grpc.ServerStreamingServer[subscriptionsv1.ExportSubscriptionsResponse]) error {
	ctx := stream.Context()
	s.log.InfoContext(ctx, "export subscriptions", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	searchSellersRequest := &sellersv1.SearchSellersRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.SearchSellersRequest_Filter{
			OwnerId:      req.GetFilter().GetOwnerId(),
			Labels:       labels,
			Region:       req.GetFilter().GetRegion(),
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
		},
	}
	searchSellersReply := new(sellersv1.SearchSellersResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", sellersSellerServiceName, "SearchSellers"), searchSellersRequest, searchSellersReply); err != nil {
		return grpcx.Wrap(err, "acme.sellers.v1.SellerService/SearchSellers")
	}
	scoreUsageRecordRequest := &subscriptionsv1.ScoreUsageRecordRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.ScoreUsageRecordRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	scoreUsageRecordReply, err := s.usageRecordValidationPort.ScoreUsageRecord(ctx, scoreUsageRecordRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordValidationService/ScoreUsageRecord")
	}
	listUserVersionsRequest := &identityv1.ListUserVersionsRequest{
		Context: req.GetContext(),
		Filter:  &identityv1.ListUserVersionsRequest_Filter{Region: req.GetFilter().GetRegion()},
	}
	listUserVersionsReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*identityv1.ListUserVersionsResponse, error) {
		return s.userHistoryClient.ListUserVersions(ctx, listUserVersionsRequest)
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.identity.v1.UserHistoryService/ListUserVersions")
	}
	if err := stream.Send(&subscriptionsv1.ExportSubscriptionsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: searchSellersReply.GetResult().GetReferenceIds(),
			Details:      listUserVersionsReply.GetResult().GetDetails(),
			Status:       "ok",
		},
		Subscription: &subscriptionsv1.Subscription{
			CurrencyCode: scoreUsageRecordReply.GetBreakdown().GetCurrencyCode(),
			Holder: &customersv1.CustomerRef{
				Email: scoreUsageRecordReply.GetUsageRecord().GetSubject().GetEmail(),
			},
			Labels: req.GetFilter().GetLabels(),
		},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizeSubscriptions summarizes a subscription.
func (s *SubscriptionQueryServer) SummarizeSubscriptions(ctx context.Context, req *subscriptionsv1.SummarizeSubscriptionsRequest) (*subscriptionsv1.SummarizeSubscriptionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "subscription", req.GetSubscriptionId(), "summarize_subscriptions"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize subscriptions", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	compareSearchQueryVersionsRequest := &searchv1.CompareSearchQueryVersionsRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		SearchQuery: &searchv1.SearchQuery{Detail: packed},
		Details:     &searchv1.CompareSearchQueryVersionsRequest_Details{Snapshot: snapshot},
	}
	compareSearchQueryVersionsReply, err := s.compareSearchQueryVersionsFn(ctx, compareSearchQueryVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryHistoryService/CompareSearchQueryVersions")
	}
	routes := map[string]func(context.Context, *subscriptionsv1.NormalizeRenewalScheduleRequest, ...grpc.CallOption) (*subscriptionsv1.NormalizeRenewalScheduleResponse, error){
		"primary":  s.renewalScheduleValidationClient.NormalizeRenewalSchedule,
		"fallback": s.renewalScheduleValidationClientFallback.NormalizeRenewalSchedule,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	normalizeRenewalScheduleRequest := &subscriptionsv1.NormalizeRenewalScheduleRequest{
		Context: req.GetContext(),
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Organizer: &identityv1.UserRef{
				DisplayName: compareSearchQueryVersionsReply.GetSearchQuery().GetSubject().GetDisplayName(),
				Email:       req.GetContext().GetActor().GetEmail(),
			},
		},
		Annotations: annotations,
	}
	normalizeRenewalScheduleReply, err := route(ctx, normalizeRenewalScheduleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.RenewalScheduleValidationService/NormalizeRenewalSchedule")
	}
	return &subscriptionsv1.SummarizeSubscriptionsResponse{
		Result: &commonv1.OperationResult{
			Details: compareSearchQueryVersionsReply.GetResult().GetDetails(),
			Message: compareSearchQueryVersionsReply.GetOutcome().GetMessage(),
			Status:  normalizeRenewalScheduleReply.GetOutcome().GetStatus(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestSubscriptions lists subscriptions a page at a time.
func (s *SubscriptionQueryServer) SuggestSubscriptions(ctx context.Context, req *subscriptionsv1.SuggestSubscriptionsRequest) (*subscriptionsv1.SuggestSubscriptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuggestSubscriptions", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "suggest subscriptions", "tenant", req.GetContext().GetTenantId())
	scoreRenewalScheduleReply, err := s.renewalScheduleValidationClient.ScoreRenewalSchedule(ctx, &subscriptionsv1.ScoreRenewalScheduleRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.ScoreRenewalScheduleRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.RenewalScheduleValidationService/ScoreRenewalSchedule")
	}
	return &subscriptionsv1.SuggestSubscriptionsResponse{
		Subscriptions: []*subscriptionsv1.Subscription{&subscriptionsv1.Subscription{
			Audit: &commonv1.AuditInfo{
				CreatedBy: scoreRenewalScheduleReply.GetRenewalSchedule().GetAudit().GetCreatedBy(),
			},
		}},
		Result: &commonv1.OperationResult{
			CorrelationId: scoreRenewalScheduleReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// PeekSubscription returns a subscription.
func (s *SubscriptionQueryServer) PeekSubscription(ctx context.Context, req *subscriptionsv1.PeekSubscriptionRequest) (*subscriptionsv1.PeekSubscriptionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "subscription", req.GetSubscriptionId(), "peek_subscription"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek subscription", "tenant", req.GetContext().GetTenantId())
	return &subscriptionsv1.PeekSubscriptionResponse{
		Subscription: &subscriptionsv1.Subscription{SubscriptionId: req.GetSubscriptionId()},
		Result:       &commonv1.OperationResult{Status: "ok"},
	}, nil
}
