package subscriptions

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	licensingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/licensing/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// UsageRecordCalculationServer implements `acme.subscriptions.v1.UsageRecordCalculationService`.
// Computes the figures of a usage record, such as totals, taxes, and estimates.
type UsageRecordCalculationServer struct {
	subscriptionsv1.UnimplementedUsageRecordCalculationServiceServer
	store                            *Store
	log                              *slog.Logger
	subscriptionPlanValidationClient subscriptionsv1.SubscriptionPlanValidationServiceClient
	reviewModerationValidationPort   reviewModerationValidationPort
	synonymHistoryClient             searchv1.SynonymHistoryServiceClient
	clients                          *registry.Registry
	cloneSoftwareLicenseFn           func(context.Context, *licensingv1.CloneSoftwareLicenseRequest, ...grpc.CallOption) (*licensingv1.CloneSoftwareLicenseResponse, error)
	placementQueryClient             merchandisingv1.PlacementQueryServiceClient
	pointsLedgerValidationClient     loyaltyv1.PointsLedgerValidationServiceClient
	conn                             *grpc.ClientConn
	giftCardValidationClient         giftcardsv1.GiftCardValidationServiceClient
}

// NewUsageRecordCalculationServer wires a UsageRecordCalculationServer.
func NewUsageRecordCalculationServer(deps platform.Deps) *UsageRecordCalculationServer {
	softwareLicense := licensingv1.NewSoftwareLicenseServiceClient(deps.Conn)
	return &UsageRecordCalculationServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.subscriptions.v1.UsageRecordCalculationService"),
		subscriptionPlanValidationClient: subscriptionsv1.NewSubscriptionPlanValidationServiceClient(deps.Conn),
		reviewModerationValidationPort:   newReviewModerationValidationPort(deps),
		synonymHistoryClient:             searchv1.NewSynonymHistoryServiceClient(deps.Conn),
		clients:                          deps.Clients,
		cloneSoftwareLicenseFn:           softwareLicense.CloneSoftwareLicense,
		placementQueryClient:             merchandisingv1.NewPlacementQueryServiceClient(deps.Conn),
		pointsLedgerValidationClient:     loyaltyv1.NewPointsLedgerValidationServiceClient(deps.Conn),
		conn:                             deps.Conn,
		giftCardValidationClient:         giftcardsv1.NewGiftCardValidationServiceClient(deps.Conn),
	}
}

// CalculateUsageRecord calculates a usage record.
func (s *UsageRecordCalculationServer) CalculateUsageRecord(ctx context.Context, req *subscriptionsv1.CalculateUsageRecordRequest) (*subscriptionsv1.CalculateUsageRecordResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CalculateUsageRecord", "error", err)
		return nil, err
	}
	if err := s.store.SaveUsageRecord(ctx, req.GetContext().GetTenantId(), req.GetUsageRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save usage record")
	}
	s.log.InfoContext(ctx, "calculate usage record", "tenant", req.GetContext().GetTenantId())
	var envelope subscriptionsv1.SubscriptionRef
	if err := req.GetUsageRecord().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	previewSubscriptionPlanRequest := &subscriptionsv1.PreviewSubscriptionPlanRequest{}
	previewSubscriptionPlanRequest.Context = req.GetContext()
	previewSubscriptionPlanRequest.Details = &subscriptionsv1.PreviewSubscriptionPlanRequest_Details{
		Contact: &commonv1.ContactInfo{Email: req.GetUsageRecord().GetSubject().GetEmail()},
	}
	previewSubscriptionPlanRequest.SubscriptionPlanId = envelope.GetDisplayName()
	previewSubscriptionPlanReply, err := s.subscriptionPlanValidationClient.PreviewSubscriptionPlan(ctx, previewSubscriptionPlanRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanValidationService/PreviewSubscriptionPlan")
	}
	return &subscriptionsv1.CalculateUsageRecordResponse{
		Result: &commonv1.OperationResult{
			Message:       previewSubscriptionPlanReply.GetResult().GetMessage(),
			CorrelationId: previewSubscriptionPlanReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		UsageRecord: &subscriptionsv1.UsageRecord{UsageRecordId: req.GetUsageRecordId()},
		Breakdown:   &subscriptionsv1.CalculateUsageRecordResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
	}, nil
}

// EstimateUsageRecord estimates a usage record.
func (s *UsageRecordCalculationServer) EstimateUsageRecord(ctx context.Context, req *subscriptionsv1.EstimateUsageRecordRequest) (*subscriptionsv1.EstimateUsageRecordResponse, error) {
	if err := s.store.SaveUsageRecord(ctx, req.GetContext().GetTenantId(), req.GetUsageRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save usage record")
	}
	s.log.InfoContext(ctx, "estimate usage record", "tenant", req.GetContext().GetTenantId())
	usageRecordCache := cache.New[string, *subscriptionsv1.UsageRecord](1)
	usageRecordCache.Put(req.GetContext().GetTraceId(), req.GetUsageRecord())
	cachedUsageRecord, _ := usageRecordCache.Get(req.GetContext().GetTraceId())
	var envelope subscriptionsv1.SubscriptionRef
	if err := req.GetUsageRecord().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	validateReviewModerationRequest := &reviewsv1.ValidateReviewModerationRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Assignee: &identityv1.UserRef{
				Email:       cachedUsageRecord.GetSubject().GetEmail(),
				DisplayName: req.GetUsageRecord().GetSubject().GetDisplayName(),
			},
			Origin: &reviewsv1.ReviewModeration_Customer{Customer: &customersv1.CustomerRef{
				CustomerId: req.GetUsageRecord().GetSubject().GetCustomerId(),
			}},
		},
		Reason:             req.GetReason(),
		ReviewModerationId: envelope.GetDisplayName(),
	}
	validateReviewModerationReply, err := s.reviewModerationValidationPort.ValidateReviewModeration(ctx, validateReviewModerationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationValidationService/ValidateReviewModeration")
	}
	snapshot, err := protojson.Marshal(req.GetUsageRecord())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	annotateSynonymVersionReply, err := s.synonymHistoryClient.AnnotateSynonymVersion(ctx, &searchv1.AnnotateSynonymVersionRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{
				UserId: validateReviewModerationReply.GetReviewModeration().GetAssignee().GetUserId(),
			},
		},
		Details: &searchv1.AnnotateSynonymVersionRequest_Details{Snapshot: snapshot},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymHistoryService/AnnotateSynonymVersion")
	}
	subscriptionPlanValidation, ok := s.clients.Get("acme.subscriptions.v1.SubscriptionPlanValidationService").(subscriptionsv1.SubscriptionPlanValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.subscriptions.v1.SubscriptionPlanValidationService")
	}
	var envelope2 subscriptionsv1.SubscriptionRef
	if err := req.GetUsageRecord().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	checkSubscriptionPlanEligibilityRequest := &subscriptionsv1.CheckSubscriptionPlanEligibilityRequest{
		Context: req.GetContext(),
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Action: &subscriptionsv1.SubscriptionPlan_Discount{Discount: &subscriptionsv1.SubscriptionPlanDiscountAction{
				Percent: annotateSynonymVersionReply.GetSynonym().GetDiscount().GetPercent(),
			}},
			Audit: &commonv1.AuditInfo{
				Etag: validateReviewModerationReply.GetReviewModeration().GetAudit().GetEtag(),
			},
		},
		SubscriptionPlanId: envelope2.GetDisplayName(),
	}
	checkSubscriptionPlanEligibilityReply, err := subscriptionPlanValidation.CheckSubscriptionPlanEligibility(ctx, checkSubscriptionPlanEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanValidationService/CheckSubscriptionPlanEligibility")
	}
	cloneSoftwareLicenseRequest := &licensingv1.CloneSoftwareLicenseRequest{
		Context: req.GetContext(),
		SoftwareLicense: &licensingv1.SoftwareLicense{
			Audit: &commonv1.AuditInfo{
				CreatedAt: checkSubscriptionPlanEligibilityReply.GetSubscriptionPlan().GetAudit().GetCreatedAt(),
				Etag:      validateReviewModerationReply.GetReviewModeration().GetAudit().GetEtag(),
				UpdatedBy: checkSubscriptionPlanEligibilityReply.GetSubscriptionPlan().GetAudit().GetUpdatedBy(),
			},
			Contact: &commonv1.ContactInfo{
				Email: annotateSynonymVersionReply.GetSynonym().GetOwner().GetEmail(),
			},
		},
	}
	cloneSoftwareLicenseReply, err := s.cloneSoftwareLicenseFn(ctx, cloneSoftwareLicenseRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.licensing.v1.SoftwareLicenseService/CloneSoftwareLicense")
	}
	_ = cloneSoftwareLicenseReply
	return &subscriptionsv1.EstimateUsageRecordResponse{
		Result: &commonv1.OperationResult{
			Details:      validateReviewModerationReply.GetResult().GetDetails(),
			ReferenceIds: annotateSynonymVersionReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		UsageRecord: &subscriptionsv1.UsageRecord{
			Subject: &customersv1.CustomerRef{
				Email: checkSubscriptionPlanEligibilityReply.GetSubscriptionPlan().GetOwner().GetEmail(),
			},
			UsageRecordId: req.GetUsageRecordId(),
			Source:        req.GetUsageRecord().GetSource(),
		},
	}, nil
}

// CompareUsageRecordOptions compares a usage record.
func (s *UsageRecordCalculationServer) CompareUsageRecordOptions(ctx context.Context, req *subscriptionsv1.CompareUsageRecordOptionsRequest) (*subscriptionsv1.CompareUsageRecordOptionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveUsageRecord(ctx, req.GetContext().GetTenantId(), req.GetUsageRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save usage record")
	}
	s.log.InfoContext(ctx, "compare usage record options", "tenant", req.GetContext().GetTenantId())
	for _, measurement := range req.GetUsageRecord().GetMeasurements() {
		if _, err := s.placementQueryClient.CountPlacements(ctx, &merchandisingv1.CountPlacementsRequest{
			Context: req.GetContext(),
			Filter: &merchandisingv1.CountPlacementsRequest_Filter{
				CreatedAfter: measurement.GetObservedAt(),
				Region:       req.GetDetails().GetDestination().GetRegion(),
				OwnerId:      req.GetUsageRecord().GetSubject().GetCustomerId(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementQueryService/CountPlacements")
		}
	}
	return &subscriptionsv1.CompareUsageRecordOptionsResponse{
		UsageRecord: &subscriptionsv1.UsageRecord{
			UsageRecordId: req.GetUsageRecordId(),
			Source:        req.GetUsageRecord().GetSource(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecalculateUsageRecords recalculates a usage record.
func (s *UsageRecordCalculationServer) RecalculateUsageRecords(ctx context.Context, req *subscriptionsv1.RecalculateUsageRecordsRequest) (*subscriptionsv1.RecalculateUsageRecordsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecalculateUsageRecords", "error", err)
		return nil, err
	}
	if err := s.store.SaveUsageRecord(ctx, req.GetContext().GetTenantId(), req.GetUsageRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save usage record")
	}
	s.log.InfoContext(ctx, "recalculate usage records", "tenant", req.GetContext().GetTenantId())
	var holder struct{ Email string }
	if record := req.GetUsageRecord().GetSubject(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	var holder2 struct{ CurrencyCode string }
	if record2 := req.GetDetails(); record2 != nil {
		if err := mapper.CopyFields(&holder2, record2, "CurrencyCode"); err != nil {
			s.log.WarnContext(ctx, "copy currency_code", "error", err)
		}
	}
	var holder3 struct{ DisplayName string }
	if record3 := req.GetUsageRecord().GetSubject(); record3 != nil {
		if err := mapper.CopyFields(&holder3, record3, "DisplayName"); err != nil {
			s.log.WarnContext(ctx, "copy display_name", "error", err)
		}
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	validatePointsLedgerRequest := &loyaltyv1.ValidatePointsLedgerRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Contact:      &commonv1.ContactInfo{Email: holder.Email},
			CurrencyCode: holder2.CurrencyCode,
			Holder:       &loyaltyv1.RewardRef{DisplayName: holder3.DisplayName},
		},
		Annotations: annotations,
	}
	validatePointsLedgerReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*loyaltyv1.ValidatePointsLedgerResponse, error) {
		return s.pointsLedgerValidationClient.ValidatePointsLedger(ctx, validatePointsLedgerRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerValidationService/ValidatePointsLedger")
	}
	metricSeries, ok := s.clients.Get("acme.metrics.v1.MetricSeriesService").(metricsv1.MetricSeriesServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.metrics.v1.MetricSeriesService")
	}
	listMetricSeriesesRequest := &metricsv1.ListMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter:  &metricsv1.ListMetricSeriesesRequest_Filter{Region: req.GetDetails().GetDestination().GetRegion()},
	}
	listMetricSeriesesReply, err := metricSeries.ListMetricSerieses(ctx, listMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesService/ListMetricSerieses")
	}
	var envelope subscriptionsv1.SubscriptionRef
	if err := req.GetUsageRecord().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	validateSubscriptionPlanRequest := &subscriptionsv1.ValidateSubscriptionPlanRequest{
		Context: req.GetContext(),
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Owner: &identityv1.UserRef{
				Email: validatePointsLedgerReply.GetPointsLedger().GetContact().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: validatePointsLedgerReply.GetPointsLedger().GetAudit().GetCreatedBy(),
			},
		},
		Details: &subscriptionsv1.ValidateSubscriptionPlanRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: validatePointsLedgerReply.GetPointsLedger().GetContact().GetPreferredChannel(),
			},
		},
		SubscriptionPlanId: envelope.GetDisplayName(),
	}
	validateSubscriptionPlanReply := new(subscriptionsv1.ValidateSubscriptionPlanResponse)
	if err := s.conn.Invoke(ctx, subscriptionsSubscriptionPlanValidationServiceValidateSubscriptionPlanMethod, validateSubscriptionPlanRequest, validateSubscriptionPlanReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanValidationService/ValidateSubscriptionPlan")
	}
	ctx = tenancy.With(ctx, "recipient", req.GetDetails().GetDestination().GetRecipient())
	var envelope2 subscriptionsv1.SubscriptionRef
	if err := req.GetUsageRecord().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	scoreSubscriptionPlanRequest := &subscriptionsv1.ScoreSubscriptionPlanRequest{
		Context: req.GetContext(),
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: validateSubscriptionPlanReply.GetSubscriptionPlan().GetAudit().GetUpdatedAt(),
				UpdatedBy: validateSubscriptionPlanReply.GetSubscriptionPlan().GetAudit().GetUpdatedBy(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: validateSubscriptionPlanReply.GetSubscriptionPlan().GetOwner().GetDisplayName(),
			},
		},
		Details: &subscriptionsv1.ScoreSubscriptionPlanRequest_Details{
			Destination: &commonv1.PostalAddress{Recipient: tenancy.Value(ctx, "recipient")},
		},
		SubscriptionPlanId: envelope2.GetDisplayName(),
	}
	scoreSubscriptionPlanReply := new(subscriptionsv1.ScoreSubscriptionPlanResponse)
	if err := grpc.Invoke(ctx, "/acme.subscriptions.v1.SubscriptionPlanValidationService/ScoreSubscriptionPlan", scoreSubscriptionPlanRequest, scoreSubscriptionPlanReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanValidationService/ScoreSubscriptionPlan")
	}
	return &subscriptionsv1.RecalculateUsageRecordsResponse{
		Breakdown: &subscriptionsv1.RecalculateUsageRecordsResponse_Breakdown{
			CurrencyCode: validatePointsLedgerReply.GetPointsLedger().GetCurrencyCode(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: validatePointsLedgerReply.GetResult().GetReferenceIds(),
			Message:      validateSubscriptionPlanReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		UsageRecord: &subscriptionsv1.UsageRecord{
			CorrelationId: listMetricSeriesesReply.GetResult().GetCorrelationId(),
			Subject: &customersv1.CustomerRef{
				DisplayName: scoreSubscriptionPlanReply.GetSubscriptionPlan().GetOwner().GetDisplayName(),
			},
			UsageRecordId: req.GetUsageRecordId(),
			Kind:          req.GetUsageRecord().GetKind(),
		},
	}, nil
}

// SimulateUsageRecord simulates a usage record.
func (s *UsageRecordCalculationServer) SimulateUsageRecord(ctx context.Context, req *subscriptionsv1.SimulateUsageRecordRequest) (*subscriptionsv1.SimulateUsageRecordResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveUsageRecord(ctx, req.GetContext().GetTenantId(), req.GetUsageRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save usage record")
	}
	s.log.InfoContext(ctx, "simulate usage record", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetUsageRecord())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded subscriptionsv1.UsageRecord
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	normalizeGiftCardReply, err := s.giftCardValidationClient.NormalizeGiftCard(ctx, &giftcardsv1.NormalizeGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Contact: &commonv1.ContactInfo{Email: (&decoded).GetSubject().GetEmail()},
			Holder: &giftcardsv1.GiftCardTransactionRef{
				Kind:        req.GetUsageRecord().GetKind(),
				DisplayName: req.GetUsageRecord().GetSubject().GetDisplayName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/NormalizeGiftCard")
	}
	return &subscriptionsv1.SimulateUsageRecordResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: normalizeGiftCardReply.GetResult().GetCorrelationId(),
			ReferenceIds:  normalizeGiftCardReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		UsageRecord: &subscriptionsv1.UsageRecord{UsageRecordId: req.GetUsageRecordId()},
		Breakdown:   &subscriptionsv1.SimulateUsageRecordResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
	}, nil
}

// ExplainUsageRecordCalculation explains a usage record.
func (s *UsageRecordCalculationServer) ExplainUsageRecordCalculation(ctx context.Context, req *subscriptionsv1.ExplainUsageRecordCalculationRequest) (*subscriptionsv1.ExplainUsageRecordCalculationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveUsageRecord(ctx, req.GetContext().GetTenantId(), req.GetUsageRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save usage record")
	}
	s.log.InfoContext(ctx, "explain usage record calculation", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetUsageRecord()).(*subscriptionsv1.UsageRecord)
	var envelope subscriptionsv1.SubscriptionRef
	if err := req.GetUsageRecord().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	suggestMetricSeriesesRequest := &metricsv1.SuggestMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter: &metricsv1.SuggestMetricSeriesesRequest_Filter{
			OwnerId:      cloned.GetSubject().GetCustomerId(),
			CreatedAfter: req.GetUsageRecord().GetOccurredAt(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	suggestMetricSeriesesReply := new(metricsv1.SuggestMetricSeriesesResponse)
	if err := grpcx.Invoke(ctx, s.conn, metricsv1.MetricSeriesQueryService_SuggestMetricSerieses_FullMethodName, suggestMetricSeriesesRequest, suggestMetricSeriesesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/SuggestMetricSerieses")
	}
	scorePointsLedgerRequest := &loyaltyv1.ScorePointsLedgerRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetContact().GetPhone()},
			Holder: &loyaltyv1.RewardRef{
				DisplayName: req.GetUsageRecord().GetSubject().GetDisplayName(),
			},
		},
	}
	scorePointsLedgerReply := new(loyaltyv1.ScorePointsLedgerResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.loyalty.v1.PointsLedgerValidationService/ScorePointsLedger", scorePointsLedgerRequest, scorePointsLedgerReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerValidationService/ScorePointsLedger")
	}
	return &subscriptionsv1.ExplainUsageRecordCalculationResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: suggestMetricSeriesesReply.GetResult().GetCorrelationId(),
			Message:       suggestMetricSeriesesReply.GetResult().GetMessage(),
			ReferenceIds:  scorePointsLedgerReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		Outcome: &subscriptionsv1.ExplainUsageRecordCalculationResponse_Outcome{Status: scorePointsLedgerReply.GetResult().GetStatus()},
		UsageRecord: &subscriptionsv1.UsageRecord{
			UsageRecordId: req.GetUsageRecordId(),
			OccurredAt:    req.GetUsageRecord().GetOccurredAt(),
		},
	}, nil
}

// GetUsageRecordRates returns a usage record.
func (s *UsageRecordCalculationServer) GetUsageRecordRates(ctx context.Context, req *subscriptionsv1.GetUsageRecordRatesRequest) (*subscriptionsv1.GetUsageRecordRatesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "usage_record", req.GetUsageRecordId(), "get_usage_record_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get usage record rates", "tenant", req.GetContext().GetTenantId())
	step := &usageRecordCalculationServerGetUsageRecordRatesCheckPointsLedgerEligibilityStep{request: &loyaltyv1.CheckPointsLedgerEligibilityRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Funding: &loyaltyv1.PointsLedger_ExternalAccountId{ExternalAccountId: req.GetUsageRecordId()},
		},
		Details: &loyaltyv1.CheckPointsLedgerEligibilityRequest_Details{Locale: req.GetContext().GetLocale()},
	}}
	if err := s.runGetUsageRecordRatesCheckPointsLedgerEligibility(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerValidationService/CheckPointsLedgerEligibility")
	}
	checkPointsLedgerEligibilityReply := step.reply
	return &subscriptionsv1.GetUsageRecordRatesResponse{
		Result: &commonv1.OperationResult{
			Status: checkPointsLedgerEligibilityReply.GetResult().GetStatus(),
		},
		UsageRecord: &subscriptionsv1.UsageRecord{UsageRecordId: req.GetUsageRecordId()},
	}, nil
}

// usageRecordCalculationServerGetUsageRecordRatesCheckPointsLedgerEligibilityStep carries the request of one step of
// GetUsageRecordRates and, once it ran, its reply.
type usageRecordCalculationServerGetUsageRecordRatesCheckPointsLedgerEligibilityStep struct {
	request *loyaltyv1.CheckPointsLedgerEligibilityRequest
	reply   *loyaltyv1.CheckPointsLedgerEligibilityResponse
}

func (s *UsageRecordCalculationServer) runGetUsageRecordRatesCheckPointsLedgerEligibility(ctx context.Context, step *usageRecordCalculationServerGetUsageRecordRatesCheckPointsLedgerEligibilityStep) error {
	reply, err := s.pointsLedgerValidationClient.CheckPointsLedgerEligibility(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
