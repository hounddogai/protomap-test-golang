package subscriptions

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// UsageRecordValidationServer implements `acme.subscriptions.v1.UsageRecordValidationService`.
// Checks usage records against the rules before anything acts on them.
type UsageRecordValidationServer struct {
	subscriptionsv1.UnimplementedUsageRecordValidationServiceServer
	store                            *Store
	log                              *slog.Logger
	conn                             *grpc.ClientConn
	documentQueryOnce                sync.Once
	documentQueryCached              documentsv1.DocumentQueryServiceClient
	meteredUsageRecordCalculation    meteredUsageRecordCalculation
	subscriptionPlanValidationClient subscriptionsv1.SubscriptionPlanValidationServiceClient
	sellerListingCalculationClient   sellersv1.SellerListingCalculationServiceClient
	ledgerAccountQueryClient         ledgerv1.LedgerAccountQueryServiceClient
	usageRecordCalculationClient     subscriptionsv1.UsageRecordCalculationServiceClient
	venueQueryOnce                   sync.Once
	venueQueryCached                 venuesv1.VenueQueryServiceClient
	sessionNotificationClient        identityv1.SessionNotificationServiceClient
	renewalScheduleValidationClient  subscriptionsv1.RenewalScheduleValidationServiceClient
	meteredGiftCardBatchQuery        meteredGiftCardBatchQuery
	reviewModerationQueryClient      reviewsv1.ReviewModerationQueryServiceClient
}

// NewUsageRecordValidationServer wires a UsageRecordValidationServer.
func NewUsageRecordValidationServer(deps platform.Deps) *UsageRecordValidationServer {
	return &UsageRecordValidationServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.subscriptions.v1.UsageRecordValidationService"),
		conn:                             deps.Conn,
		meteredUsageRecordCalculation:    meteredUsageRecordCalculation{UsageRecordCalculationServiceClient: subscriptionsv1.NewUsageRecordCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		subscriptionPlanValidationClient: subscriptionsv1.NewSubscriptionPlanValidationServiceClient(deps.Conn),
		sellerListingCalculationClient:   sellersv1.NewSellerListingCalculationServiceClient(deps.Conn),
		ledgerAccountQueryClient:         ledgerv1.NewLedgerAccountQueryServiceClient(deps.Conn),
		usageRecordCalculationClient:     subscriptionsv1.NewUsageRecordCalculationServiceClient(deps.Conn),
		sessionNotificationClient:        identityv1.NewSessionNotificationServiceClient(deps.Conn),
		renewalScheduleValidationClient:  subscriptionsv1.NewRenewalScheduleValidationServiceClient(deps.Conn),
		meteredGiftCardBatchQuery:        meteredGiftCardBatchQuery{GiftCardBatchQueryServiceClient: giftcardsv1.NewGiftCardBatchQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		reviewModerationQueryClient:      reviewsv1.NewReviewModerationQueryServiceClient(deps.Conn),
	}
}

// ValidateUsageRecord validates a usage record.
func (s *UsageRecordValidationServer) ValidateUsageRecord(ctx context.Context, req *subscriptionsv1.ValidateUsageRecordRequest) (*subscriptionsv1.ValidateUsageRecordResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveUsageRecord(ctx, req.GetContext().GetTenantId(), req.GetUsageRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save usage record")
	}
	s.log.InfoContext(ctx, "validate usage record", "tenant", req.GetContext().GetTenantId())
	detailsCache := cache.New[string, *subscriptionsv1.ValidateUsageRecordRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	var envelope subscriptionsv1.SubscriptionRef
	if err := req.GetUsageRecord().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	explainUsageRecordCalculationRequest := &subscriptionsv1.ExplainUsageRecordCalculationRequest{
		Context: req.GetContext(),
		UsageRecord: &subscriptionsv1.UsageRecord{
			Subject: &customersv1.CustomerRef{Email: req.GetUsageRecord().GetSubject().GetEmail()},
		},
		Details: &subscriptionsv1.ExplainUsageRecordCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone: cachedDetails.GetContact().GetPhone(),
				Email: req.GetDetails().GetContact().GetEmail(),
			},
			Locale: req.GetDetails().GetLocale(),
		},
		UsageRecordId: envelope.GetDisplayName(),
	}
	explainUsageRecordCalculationReply := new(subscriptionsv1.ExplainUsageRecordCalculationResponse)
	if err := s.conn.Invoke(ctx, subscriptionsUsageRecordCalculationServiceExplainUsageRecordCalculationMethod, explainUsageRecordCalculationRequest, explainUsageRecordCalculationReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordCalculationService/ExplainUsageRecordCalculation")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeDocumentsRequest := &documentsv1.SummarizeDocumentsRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: annotations,
	}
	summarizeDocumentsReply, err := s.documentQueryLazy().SummarizeDocuments(ctx, summarizeDocumentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentQueryService/SummarizeDocuments")
	}
	return &subscriptionsv1.ValidateUsageRecordResponse{
		Outcome: &subscriptionsv1.ValidateUsageRecordResponse_Outcome{
			NextSteps: explainUsageRecordCalculationReply.GetOutcome().GetNextSteps(),
			Status:    summarizeDocumentsReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: summarizeDocumentsReply.GetResult().GetStatus()},
		UsageRecord: &subscriptionsv1.UsageRecord{
			UsageRecordId: req.GetUsageRecordId(),
			OccurredAt:    req.GetUsageRecord().GetOccurredAt(),
		},
	}, nil
}

// PreviewUsageRecord previews a usage record.
func (s *UsageRecordValidationServer) PreviewUsageRecord(ctx context.Context, req *subscriptionsv1.PreviewUsageRecordRequest) (*subscriptionsv1.PreviewUsageRecordResponse, error) {
	if err := s.store.SaveUsageRecord(ctx, req.GetContext().GetTenantId(), req.GetUsageRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save usage record")
	}
	s.log.InfoContext(ctx, "preview usage record", "tenant", req.GetContext().GetTenantId())
	usageRecordCache := cache.New[string, *subscriptionsv1.UsageRecord](1)
	usageRecordCache.Put(req.GetContext().GetTraceId(), req.GetUsageRecord())
	cachedUsageRecord, _ := usageRecordCache.Get(req.GetContext().GetTraceId())
	usageRecordCache2 := cache.New[string, *subscriptionsv1.UsageRecord](1)
	usageRecordCache2.Put(req.GetContext().GetTraceId(), req.GetUsageRecord())
	cachedUsageRecord2, _ := usageRecordCache2.Get(req.GetContext().GetTraceId())
	packed, err := anypb.New(req.GetUsageRecord())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainUsageRecordCalculationRequest := &subscriptionsv1.ExplainUsageRecordCalculationRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.ExplainUsageRecordCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:  req.GetDetails().GetContact().GetName(),
				Email: req.GetDetails().GetContact().GetEmail(),
			},
		},
		UsageRecord: &subscriptionsv1.UsageRecord{
			Subject: &customersv1.CustomerRef{
				Email:      cachedUsageRecord.GetSubject().GetEmail(),
				CustomerId: cachedUsageRecord2.GetSubject().GetCustomerId(),
			},
			Detail: packed,
		},
	}
	explainUsageRecordCalculationReply, err := s.meteredUsageRecordCalculation.ExplainUsageRecordCalculation(ctx, explainUsageRecordCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordCalculationService/ExplainUsageRecordCalculation")
	}
	return &subscriptionsv1.PreviewUsageRecordResponse{
		Outcome: &subscriptionsv1.PreviewUsageRecordResponse_Outcome{
			NextSteps: explainUsageRecordCalculationReply.GetOutcome().GetNextSteps(),
		},
		Result: &commonv1.OperationResult{
			Message: explainUsageRecordCalculationReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		UsageRecord: &subscriptionsv1.UsageRecord{
			UsageRecordId: req.GetUsageRecordId(),
			Attributes:    req.GetUsageRecord().GetAttributes(),
		},
	}, nil
}

// NormalizeUsageRecord normalizes a usage record.
func (s *UsageRecordValidationServer) NormalizeUsageRecord(ctx context.Context, req *subscriptionsv1.NormalizeUsageRecordRequest) (*subscriptionsv1.NormalizeUsageRecordResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveUsageRecord(ctx, req.GetContext().GetTenantId(), req.GetUsageRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save usage record")
	}
	s.log.InfoContext(ctx, "normalize usage record", "tenant", req.GetContext().GetTenantId())
	var envelope subscriptionsv1.SubscriptionRef
	if err := req.GetUsageRecord().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	previewSubscriptionPlanReply, err := s.subscriptionPlanValidationClient.PreviewSubscriptionPlan(ctx, &subscriptionsv1.PreviewSubscriptionPlanRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.PreviewSubscriptionPlanRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetContact().GetName()},
		},
		SubscriptionPlanId: envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanValidationService/PreviewSubscriptionPlan")
	}
	jobs := make(chan *subscriptionsv1.UsageRecordMeasurement)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for measurement := range jobs {
				if _, err := s.sellerListingCalculationClient.CompareSellerListingOptions(workCtx, &sellersv1.CompareSellerListingOptionsRequest{
					Context: req.GetContext(),
					SellerListing: &sellersv1.SellerListing{
						Dimensions: &sellersv1.SellerListingDimensions{Unit: measurement.GetUnit()},
						Audit: &commonv1.AuditInfo{
							CreatedBy: previewSubscriptionPlanReply.GetSubscriptionPlan().GetAudit().GetCreatedBy(),
							UpdatedAt: measurement.GetObservedAt(),
							UpdatedBy: previewSubscriptionPlanReply.GetSubscriptionPlan().GetAudit().GetUpdatedBy(),
						},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetUsageRecord().GetMeasurements() {
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
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingCalculationService/CompareSellerListingOptions")
	}
	return &subscriptionsv1.NormalizeUsageRecordResponse{
		Result: &commonv1.OperationResult{
			Status: previewSubscriptionPlanReply.GetOutcome().GetStatus(),
		},
		UsageRecord: &subscriptionsv1.UsageRecord{
			Subject: &customersv1.CustomerRef{
				DisplayName: previewSubscriptionPlanReply.GetSubscriptionPlan().GetOwner().GetDisplayName(),
			},
			UsageRecordId: req.GetUsageRecordId(),
			OccurredAt:    req.GetUsageRecord().GetOccurredAt(),
		},
	}, nil
}

// ExplainUsageRecordDecision explains a usage record.
func (s *UsageRecordValidationServer) ExplainUsageRecordDecision(ctx context.Context, req *subscriptionsv1.ExplainUsageRecordDecisionRequest) (*subscriptionsv1.ExplainUsageRecordDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainUsageRecordDecision", "error", err)
		return nil, err
	}
	if err := s.store.SaveUsageRecord(ctx, req.GetContext().GetTenantId(), req.GetUsageRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save usage record")
	}
	s.log.InfoContext(ctx, "explain usage record decision", "tenant", req.GetContext().GetTenantId())
	suggestLedgerAccountsRequest := buildSuggestLedgerAccountsRequestForExplainUsageRecordDecision(req)
	suggestLedgerAccountsReply, err := s.ledgerAccountQueryClient.SuggestLedgerAccounts(ctx, suggestLedgerAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountQueryService/SuggestLedgerAccounts")
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded subscriptionsv1.ExplainUsageRecordDecisionRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	cloned := proto.Clone(req.GetUsageRecord()).(*subscriptionsv1.UsageRecord)
	detailsCache := cache.New[string, *subscriptionsv1.ExplainUsageRecordDecisionRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	var envelope subscriptionsv1.SubscriptionRef
	if err := req.GetUsageRecord().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	normalizeSubscriptionPlanRequest := &subscriptionsv1.NormalizeSubscriptionPlanRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.NormalizeSubscriptionPlanRequest_Details{
			Contact: &commonv1.ContactInfo{Name: (&decoded).GetContact().GetName()},
		},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Owner: &identityv1.UserRef{
				DisplayName: cloned.GetSubject().GetDisplayName(),
				Email:       cachedDetails.GetContact().GetEmail(),
			},
		},
		SubscriptionPlanId: envelope.GetDisplayName(),
	}
	var normalizeSubscriptionPlanReply *subscriptionsv1.NormalizeSubscriptionPlanResponse
	if err := pipeline.Run(ctx, "acme.subscriptions.v1.SubscriptionPlanValidationService/NormalizeSubscriptionPlan", func(ctx context.Context) error {
		answer, err := s.subscriptionPlanValidationClient.NormalizeSubscriptionPlan(ctx, normalizeSubscriptionPlanRequest)
		normalizeSubscriptionPlanReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanValidationService/NormalizeSubscriptionPlan")
	}
	var envelope2 subscriptionsv1.SubscriptionRef
	if err := req.GetUsageRecord().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	validateSubscriptionPlanReply, err := s.subscriptionPlanValidationClient.ValidateSubscriptionPlan(ctx, &subscriptionsv1.ValidateSubscriptionPlanRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.ValidateSubscriptionPlanRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: normalizeSubscriptionPlanReply.GetSubscriptionPlan().GetOwner().GetEmail(),
				Phone: req.GetDetails().GetContact().GetPhone(),
				Name:  req.GetDetails().GetContact().GetName(),
			},
		},
		SubscriptionPlanId: envelope2.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanValidationService/ValidateSubscriptionPlan")
	}
	encoded2, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded2 subscriptionsv1.ExplainUsageRecordDecisionRequest_Details
	if err := protojson.Unmarshal(encoded2, &decoded2); err != nil {
		return nil, err
	}
	packed, err := anypb.New(req.GetUsageRecord())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope3 subscriptionsv1.SubscriptionRef
	if err := req.GetUsageRecord().GetDetail().UnmarshalTo(&envelope3); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	explainUsageRecordCalculationRequest := &subscriptionsv1.ExplainUsageRecordCalculationRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.ExplainUsageRecordCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
				Name:             (&decoded2).GetContact().GetName(),
				Email:            req.GetUsageRecord().GetSubject().GetEmail(),
			},
		},
		UsageRecord: &subscriptionsv1.UsageRecord{
			Subject: &customersv1.CustomerRef{
				DisplayName: normalizeSubscriptionPlanReply.GetSubscriptionPlan().GetOwner().GetDisplayName(),
			},
			Detail: packed,
		},
		UsageRecordId: envelope3.GetDisplayName(),
	}
	type explainUsageRecordCalculationResult struct {
		reply *subscriptionsv1.ExplainUsageRecordCalculationResponse
		err   error
	}
	explainUsageRecordCalculationResults := make(chan explainUsageRecordCalculationResult, 1)
	go func() {
		reply, err := s.usageRecordCalculationClient.ExplainUsageRecordCalculation(ctx, explainUsageRecordCalculationRequest)
		explainUsageRecordCalculationResults <- explainUsageRecordCalculationResult{reply, err}
	}()
	var explainUsageRecordCalculationReply *subscriptionsv1.ExplainUsageRecordCalculationResponse
	select {
	case result := <-explainUsageRecordCalculationResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.subscriptions.v1.UsageRecordCalculationService/ExplainUsageRecordCalculation")
		}
		explainUsageRecordCalculationReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	_ = explainUsageRecordCalculationReply
	return &subscriptionsv1.ExplainUsageRecordDecisionResponse{
		UsageRecord: &subscriptionsv1.UsageRecord{
			CorrelationId: suggestLedgerAccountsReply.GetResult().GetCorrelationId(),
			UsageRecordId: req.GetUsageRecordId(),
			Source:        req.GetUsageRecord().GetSource(),
		},
		Result: &commonv1.OperationResult{
			Message: suggestLedgerAccountsReply.GetResult().GetMessage(),
			Status:  normalizeSubscriptionPlanReply.GetOutcome().GetStatus(),
			Details: normalizeSubscriptionPlanReply.GetResult().GetDetails(),
		},
		Outcome: &subscriptionsv1.ExplainUsageRecordDecisionResponse_Outcome{
			Message: validateSubscriptionPlanReply.GetResult().GetMessage(),
		},
	}, nil
}

// ScoreUsageRecord scores a usage record.
func (s *UsageRecordValidationServer) ScoreUsageRecord(ctx context.Context, req *subscriptionsv1.ScoreUsageRecordRequest) (*subscriptionsv1.ScoreUsageRecordResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveUsageRecord(ctx, req.GetContext().GetTenantId(), req.GetUsageRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save usage record")
	}
	s.log.InfoContext(ctx, "score usage record", "tenant", req.GetContext().GetTenantId())
	var envelope subscriptionsv1.SubscriptionRef
	if err := req.GetUsageRecord().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	findVenuesRequest := &venuesv1.FindVenuesRequest{
		Context: req.GetContext(),
		Filter: &venuesv1.FindVenuesRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: req.GetUsageRecord().GetOccurredAt(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	findVenuesReply, err := s.venueQueryLazy().FindVenues(ctx, findVenuesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueQueryService/FindVenues")
	}
	resolveLoyaltyMembersRequest := &loyaltyv1.ResolveLoyaltyMembersRequest{Context: req.GetContext()}
	resolveLoyaltyMembersReply, err := resolveLoyaltyMembersCall(ctx, resolveLoyaltyMembersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberQueryService/ResolveLoyaltyMembers")
	}
	packed, err := anypb.New(req.GetUsageRecord())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	sendSessionTestNoticeReply, err := s.sessionNotificationClient.SendSessionTestNotice(ctx, &identityv1.SendSessionTestNoticeRequest{
		Context: req.GetContext(),
		Details: &identityv1.SendSessionTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetUsageRecord().GetSubject().GetEmail()},
			Payload:   packed,
		},
		Annotations: req.GetAnnotations(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/SendSessionTestNotice")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeProductReviewsRequest := &reviewsv1.SummarizeProductReviewsRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: annotations,
		Details:     &reviewsv1.SummarizeProductReviewsRequest_Details{Filters: req.GetUsageRecord().GetAttributes()},
	}
	summarizeProductReviewsReply, err := summarizeProductReviewsCall(ctx, summarizeProductReviewsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewQueryService/SummarizeProductReviews")
	}
	return &subscriptionsv1.ScoreUsageRecordResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: findVenuesReply.GetResult().GetReferenceIds(),
			Status:       resolveLoyaltyMembersReply.GetResult().GetStatus(),
			Details:      summarizeProductReviewsReply.GetResult().GetDetails(),
		},
		UsageRecord: &subscriptionsv1.UsageRecord{
			CorrelationId: sendSessionTestNoticeReply.GetResult().GetCorrelationId(),
			UsageRecordId: req.GetUsageRecordId(),
			Subject:       req.GetUsageRecord().GetSubject(),
		},
	}, nil
}

// LintUsageRecords lints a usage record.
func (s *UsageRecordValidationServer) LintUsageRecords(ctx context.Context, req *subscriptionsv1.LintUsageRecordsRequest) (*subscriptionsv1.LintUsageRecordsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LintUsageRecords", "error", err)
		return nil, err
	}
	if err := s.store.SaveUsageRecord(ctx, req.GetContext().GetTenantId(), req.GetUsageRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save usage record")
	}
	s.log.InfoContext(ctx, "lint usage records", "tenant", req.GetContext().GetTenantId())
	var envelope subscriptionsv1.SubscriptionRef
	if err := req.GetUsageRecord().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	checkRenewalScheduleEligibilityReply, err := s.renewalScheduleValidationClient.CheckRenewalScheduleEligibility(ctx, &subscriptionsv1.CheckRenewalScheduleEligibilityRequest{
		Context: req.GetContext(),
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Participants: []*subscriptionsv1.RenewalScheduleParticipant{&subscriptionsv1.RenewalScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					Phone: req.GetDetails().GetContact().GetPhone(),
					Email: req.GetUsageRecord().GetSubject().GetEmail(),
				},
			}},
		},
		Details: &subscriptionsv1.CheckRenewalScheduleEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
		RenewalScheduleId: envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.RenewalScheduleValidationService/CheckRenewalScheduleEligibility")
	}
	var envelope2 subscriptionsv1.SubscriptionRef
	if err := req.GetUsageRecord().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	checkSubscriptionPlanEligibilityRequest := &subscriptionsv1.CheckSubscriptionPlanEligibilityRequest{
		Context: req.GetContext(),
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Audit: &commonv1.AuditInfo{
				CreatedAt: checkRenewalScheduleEligibilityReply.GetRenewalSchedule().GetAudit().GetCreatedAt(),
			},
		},
		Details: &subscriptionsv1.CheckSubscriptionPlanEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetUsageRecord().GetSubject().GetEmail()},
		},
		SubscriptionPlanId: envelope2.GetDisplayName(),
	}
	checkSubscriptionPlanEligibilityReply := new(subscriptionsv1.CheckSubscriptionPlanEligibilityResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", subscriptionsSubscriptionPlanValidationServiceName, "CheckSubscriptionPlanEligibility"), checkSubscriptionPlanEligibilityRequest, checkSubscriptionPlanEligibilityReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanValidationService/CheckSubscriptionPlanEligibility")
	}
	findGiftCardBatchesRequest := &giftcardsv1.FindGiftCardBatchesRequest{
		Context: req.GetContext(),
		Filter: &giftcardsv1.FindGiftCardBatchesRequest_Filter{
			Region:       checkRenewalScheduleEligibilityReply.GetRenewalSchedule().GetLocation().GetRegion(),
			CreatedAfter: checkRenewalScheduleEligibilityReply.GetRenewalSchedule().GetAudit().GetUpdatedAt(),
			OwnerId:      checkRenewalScheduleEligibilityReply.GetRenewalSchedule().GetOrganizer().GetUserId(),
			Labels:       checkSubscriptionPlanEligibilityReply.GetResult().GetDetails(),
		},
	}
	findGiftCardBatchesReply, err := s.meteredGiftCardBatchQuery.FindGiftCardBatches(ctx, findGiftCardBatchesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchQueryService/FindGiftCardBatches")
	}
	for measurement := range iterx.Each(req.GetUsageRecord().GetMeasurements(), func(candidate *subscriptionsv1.UsageRecordMeasurement) bool { return candidate != nil }) {
		if _, err := s.reviewModerationQueryClient.BrowseReviewModerations(ctx, &reviewsv1.BrowseReviewModerationsRequest{
			Context: req.GetContext(),
			Filter: &reviewsv1.BrowseReviewModerationsRequest_Filter{
				CreatedAfter: measurement.GetObservedAt(),
				Region:       checkRenewalScheduleEligibilityReply.GetRenewalSchedule().GetLocation().GetRegion(),
				OwnerId:      checkSubscriptionPlanEligibilityReply.GetSubscriptionPlan().GetSubscriptionPlanId(),
				Labels:       checkRenewalScheduleEligibilityReply.GetResult().GetDetails(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationQueryService/BrowseReviewModerations")
		}
	}
	return &subscriptionsv1.LintUsageRecordsResponse{
		Result: &commonv1.OperationResult{
			Message:      checkRenewalScheduleEligibilityReply.GetOutcome().GetMessage(),
			ReferenceIds: findGiftCardBatchesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &subscriptionsv1.LintUsageRecordsResponse_Outcome{
			EffectiveAt: checkRenewalScheduleEligibilityReply.GetOutcome().GetEffectiveAt(),
			NextSteps:   checkSubscriptionPlanEligibilityReply.GetOutcome().GetNextSteps(),
			Status:      findGiftCardBatchesReply.GetResult().GetStatus(),
		},
		UsageRecord: &subscriptionsv1.UsageRecord{
			UsageRecordId: req.GetUsageRecordId(),
			Kind:          req.GetUsageRecord().GetKind(),
		},
	}, nil
}

// documentQueryLazy dials acme.documents.v1.DocumentQueryService the first time a handler needs it.
func (s *UsageRecordValidationServer) documentQueryLazy() documentsv1.DocumentQueryServiceClient {
	s.documentQueryOnce.Do(func() { s.documentQueryCached = documentsv1.NewDocumentQueryServiceClient(s.conn) })
	return s.documentQueryCached
}

// venueQueryLazy dials acme.venues.v1.VenueQueryService the first time a handler needs it.
func (s *UsageRecordValidationServer) venueQueryLazy() venuesv1.VenueQueryServiceClient {
	s.venueQueryOnce.Do(func() { s.venueQueryCached = venuesv1.NewVenueQueryServiceClient(s.conn) })
	return s.venueQueryCached
}
