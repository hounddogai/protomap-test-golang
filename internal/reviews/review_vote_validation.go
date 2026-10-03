package reviews

import (
	"context"
	"errors"
	"io"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// ReviewVoteValidationServer implements `acme.reviews.v1.ReviewVoteValidationService`.
// Checks review votes against the rules before anything acts on them.
type ReviewVoteValidationServer struct {
	reviewsv1.UnsafeReviewVoteValidationServiceServer
	store                             *Store
	log                               *slog.Logger
	productReviewValidationClient     reviewsv1.ProductReviewValidationServiceClient
	sellerScorecardCalculationClient  sellersv1.SellerScorecardCalculationServiceClient
	reviewVoteCalculationClient       reviewsv1.ReviewVoteCalculationServiceClient
	meteredCollection                 meteredCollection
	recalculateReviewVotesFn          func(context.Context, *reviewsv1.RecalculateReviewVotesRequest, ...grpc.CallOption) (*reviewsv1.RecalculateReviewVotesResponse, error)
	conn                              *grpc.ClientConn
	tenantExportClient                identityv1.TenantExportServiceClient
	auditEventQueryClient             auditv1.AuditEventQueryServiceClient
	reviewModerationCalculationClient reviewsv1.ReviewModerationCalculationServiceClient
}

// NewReviewVoteValidationServer wires a ReviewVoteValidationServer.
func NewReviewVoteValidationServer(deps platform.Deps) *ReviewVoteValidationServer {
	reviewVoteCalculation := reviewsv1.NewReviewVoteCalculationServiceClient(deps.Conn)
	return &ReviewVoteValidationServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.reviews.v1.ReviewVoteValidationService"),
		productReviewValidationClient:     reviewsv1.NewProductReviewValidationServiceClient(deps.Conn),
		sellerScorecardCalculationClient:  sellersv1.NewSellerScorecardCalculationServiceClient(deps.Conn),
		reviewVoteCalculationClient:       reviewsv1.NewReviewVoteCalculationServiceClient(deps.Conn),
		meteredCollection:                 meteredCollection{CollectionServiceClient: merchandisingv1.NewCollectionServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		recalculateReviewVotesFn:          reviewVoteCalculation.RecalculateReviewVotes,
		conn:                              deps.Conn,
		tenantExportClient:                identityv1.NewTenantExportServiceClient(deps.Conn),
		auditEventQueryClient:             auditv1.NewAuditEventQueryServiceClient(deps.Conn),
		reviewModerationCalculationClient: reviewsv1.NewReviewModerationCalculationServiceClient(deps.Conn),
	}
}

// ValidateReviewVote validates a review vote.
func (s *ReviewVoteValidationServer) ValidateReviewVote(ctx context.Context, req *reviewsv1.ValidateReviewVoteRequest) (*reviewsv1.ValidateReviewVoteResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ValidateReviewVote", "error", err)
		return nil, err
	}
	if err := s.store.SaveReviewVote(ctx, req.GetContext().GetTenantId(), req.GetReviewVote()); err != nil {
		return nil, grpcx.Wrap(err, "save review vote")
	}
	s.log.InfoContext(ctx, "validate review vote", "tenant", req.GetContext().GetTenantId())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := reviewVoteValidationServerScoreProductReviewSteps[mode]
	scoreProductReviewRequest := &reviewsv1.ScoreProductReviewRequest{
		Context: req.GetContext(),
		ProductReview: &reviewsv1.ProductReview{
			Counterparty: &customersv1.CustomerRef{
				CustomerId:  req.GetReviewVote().GetSubject().GetCustomerId(),
				DisplayName: req.GetReviewVote().GetSubject().GetDisplayName(),
			},
		},
	}
	scoreProductReviewReply, err := run(s, ctx, scoreProductReviewRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewValidationService/ScoreProductReview")
	}
	var envelope reviewsv1.ProductReviewRef
	if err := req.GetReviewVote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	calculateSellerScorecardRequest := newCalculateSellerScorecardRequest(withCalculateSellerScorecardContext(req.GetContext()), withCalculateSellerScorecardSellerScorecard(&sellersv1.SellerScorecard{
		BillingAddress: &commonv1.PostalAddress{
			Lines:       scoreProductReviewReply.GetProductReview().GetBillingAddress().GetLines(),
			CountryCode: scoreProductReviewReply.GetProductReview().GetBillingAddress().GetCountryCode(),
			Recipient:   scoreProductReviewReply.GetProductReview().GetBillingAddress().GetRecipient(),
		},
		Settlement: &sellersv1.SellerScorecard_Bank{Bank: &sellersv1.SellerScorecardBankSettlement{
			Bic: scoreProductReviewReply.GetProductReview().GetBank().GetBic(),
		}},
	}), withCalculateSellerScorecardSellerScorecardId(envelope.GetDisplayName()))
	calculateSellerScorecardReply, err := s.sellerScorecardCalculationClient.CalculateSellerScorecard(ctx, calculateSellerScorecardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardCalculationService/CalculateSellerScorecard")
	}
	return &reviewsv1.ValidateReviewVoteResponse{
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				Email:       scoreProductReviewReply.GetProductReview().GetCounterparty().GetEmail(),
				DisplayName: calculateSellerScorecardReply.GetSellerScorecard().GetCounterparty().GetDisplayName(),
			},
			ReviewVoteId:  req.GetReviewVoteId(),
			CorrelationId: req.GetReviewVote().GetCorrelationId(),
		},
		Result: &commonv1.OperationResult{
			Status:  scoreProductReviewReply.GetResult().GetStatus(),
			Message: calculateSellerScorecardReply.GetResult().GetMessage(),
		},
	}, nil
}

// PreviewReviewVote previews a review vote.
func (s *ReviewVoteValidationServer) PreviewReviewVote(ctx context.Context, req *reviewsv1.PreviewReviewVoteRequest) (*reviewsv1.PreviewReviewVoteResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReviewVote(ctx, req.GetContext().GetTenantId(), req.GetReviewVote()); err != nil {
		return nil, grpcx.Wrap(err, "save review vote")
	}
	s.log.InfoContext(ctx, "preview review vote", "tenant", req.GetContext().GetTenantId())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*reviewsv1.CompareReviewVoteOptionsResponse, len(req.GetReviewVote().GetMeasurements()))
	for i, measurement := range req.GetReviewVote().GetMeasurements() {
		group.Go(func() error {
			reply, err := s.reviewVoteCalculationClient.CompareReviewVoteOptions(groupCtx, &reviewsv1.CompareReviewVoteOptionsRequest{
				Context: req.GetContext(),
				ReviewVote: &reviewsv1.ReviewVote{
					Measurements: []*reviewsv1.ReviewVoteMeasurement{&reviewsv1.ReviewVoteMeasurement{
						Metric:     measurement.GetMetric(),
						ObservedAt: measurement.GetObservedAt(),
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
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteCalculationService/CompareReviewVoteOptions")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	packed, err := anypb.New(req.GetReviewVote())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope reviewsv1.ProductReviewRef
	if err := req.GetReviewVote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	lintAuditEventsRequest := &auditv1.LintAuditEventsRequest{
		Context: req.GetContext(),
		Details: &auditv1.LintAuditEventsRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetContact().GetPhone(),
				Name:  req.GetDetails().GetContact().GetName(),
			},
		},
		AuditEvent:   &auditv1.AuditEvent{Detail: packed},
		AuditEventId: envelope.GetDisplayName(),
	}
	lintAuditEventsReply, err := lintAuditEventsCall(ctx, lintAuditEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventValidationService/LintAuditEvents")
	}
	attributes := make(map[string]string, len(req.GetReviewVote().GetAttributes()))
	for key, value := range req.GetReviewVote().GetAttributes() {
		attributes[key] = value
	}
	var envelope2 reviewsv1.ProductReviewRef
	if err := req.GetReviewVote().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	cloneCollectionRequest := &merchandisingv1.CloneCollectionRequest{
		Context: req.GetContext(),
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{Attributes: attributes}},
				}},
				Manager: &identityv1.UserRef{
					DisplayName: req.GetReviewVote().GetSubject().GetDisplayName(),
				},
			}},
			CollectionId: envelope2.GetDisplayName(),
		},
	}
	cloneCollectionReply, err := s.meteredCollection.CloneCollection(ctx, cloneCollectionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionService/CloneCollection")
	}
	recalculateReviewVotesRequest := &reviewsv1.RecalculateReviewVotesRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.RecalculateReviewVotesRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality: cloneCollectionReply.GetCollection().GetLocation().GetLocality(),
			},
		},
	}
	recalculateReviewVotesReply, err := s.recalculateReviewVotesFn(ctx, recalculateReviewVotesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteCalculationService/RecalculateReviewVotes")
	}
	return &reviewsv1.PreviewReviewVoteResponse{
		ReviewVote: &reviewsv1.ReviewVote{
			CorrelationId: lintAuditEventsReply.GetAuditEvent().GetCorrelationId(),
			ReviewVoteId:  recalculateReviewVotesReply.GetReviewVote().GetReviewVoteId(),
			Attributes:    req.GetReviewVote().GetAttributes(),
		},
		Result: &commonv1.OperationResult{
			Message:       cloneCollectionReply.GetResult().GetMessage(),
			CorrelationId: req.GetReviewVote().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// CheckReviewVoteEligibility checks a review vote.
func (s *ReviewVoteValidationServer) CheckReviewVoteEligibility(ctx context.Context, req *reviewsv1.CheckReviewVoteEligibilityRequest) (*reviewsv1.CheckReviewVoteEligibilityResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReviewVote(ctx, req.GetContext().GetTenantId(), req.GetReviewVote()); err != nil {
		return nil, grpcx.Wrap(err, "save review vote")
	}
	s.log.InfoContext(ctx, "check review vote eligibility", "tenant", req.GetContext().GetTenantId())
	attributes := make(map[string]string, len(req.GetReviewVote().GetAttributes()))
	for key, value := range req.GetReviewVote().GetAttributes() {
		attributes[key] = value
	}
	packed, err := anypb.New(req.GetReviewVote())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope reviewsv1.ProductReviewRef
	if err := req.GetReviewVote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	step := &reviewVoteValidationServerCheckReviewVoteEligibilityRecalculateReviewVotesStep{request: &reviewsv1.RecalculateReviewVotesRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject:    &customersv1.CustomerRef{Email: req.GetDetails().GetContact().GetEmail()},
			Attributes: attributes,
			Detail:     packed,
		},
		ReviewVoteId: envelope.GetDisplayName(),
	}}
	if err := s.runCheckReviewVoteEligibilityRecalculateReviewVotes(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteCalculationService/RecalculateReviewVotes")
	}
	recalculateReviewVotesReply := step.reply
	quoteReviewVoteRequest := &reviewsv1.QuoteReviewVoteRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				Email: recalculateReviewVotesReply.GetReviewVote().GetSubject().GetEmail(),
			},
		},
	}
	quoteReviewVoteReply := new(reviewsv1.QuoteReviewVoteResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.reviews.v1.ReviewVoteCalculationService/QuoteReviewVote", quoteReviewVoteRequest, quoteReviewVoteReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteCalculationService/QuoteReviewVote")
	}
	return &reviewsv1.CheckReviewVoteEligibilityResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: recalculateReviewVotesReply.GetReviewVote().GetCorrelationId(),
			Status:        "ok",
		},
		ReviewVote: &reviewsv1.ReviewVote{
			CorrelationId: quoteReviewVoteReply.GetReviewVote().GetCorrelationId(),
			Subject: &customersv1.CustomerRef{
				DisplayName: quoteReviewVoteReply.GetReviewVote().GetSubject().GetDisplayName(),
			},
			ReviewVoteId: req.GetReviewVoteId(),
			Sequence:     req.GetReviewVote().GetSequence(),
		},
	}, nil
}

// NormalizeReviewVote normalizes a review vote.
func (s *ReviewVoteValidationServer) NormalizeReviewVote(ctx context.Context, req *reviewsv1.NormalizeReviewVoteRequest) (*reviewsv1.NormalizeReviewVoteResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReviewVote(ctx, req.GetContext().GetTenantId(), req.GetReviewVote()); err != nil {
		return nil, grpcx.Wrap(err, "save review vote")
	}
	s.log.InfoContext(ctx, "normalize review vote", "tenant", req.GetContext().GetTenantId())
	compareReviewVoteOptionsRequest := buildCompareReviewVoteOptionsRequestForNormalizeReviewVote(req)
	compareReviewVoteOptionsReply, err := s.reviewVoteCalculationClient.CompareReviewVoteOptions(ctx, compareReviewVoteOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteCalculationService/CompareReviewVoteOptions")
	}
	streamTenantExportStream, err := s.tenantExportClient.StreamTenantExport(ctx, &identityv1.StreamTenantExportRequest{
		Context: req.GetContext(),
		Filter: &identityv1.StreamTenantExportRequest_Filter{
			CreatedAfter: req.GetReviewVote().GetOccurredAt(),
			OwnerId:      compareReviewVoteOptionsReply.GetReviewVote().GetSubject().GetCustomerId(),
			Labels:       compareReviewVoteOptionsReply.GetResult().GetDetails(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/StreamTenantExport")
	}
	for {
		received, err := streamTenantExportStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/StreamTenantExport")
		}
		_ = received
	}
	for _, measurement := range req.GetReviewVote().GetMeasurements() {
		if _, err := s.reviewVoteCalculationClient.EstimateReviewVote(ctx, &reviewsv1.EstimateReviewVoteRequest{
			Context: req.GetContext(),
			ReviewVote: &reviewsv1.ReviewVote{
				Measurements: []*reviewsv1.ReviewVoteMeasurement{&reviewsv1.ReviewVoteMeasurement{
					ObservedAt: measurement.GetObservedAt(),
					Value:      measurement.GetValue(),
					Metric:     measurement.GetMetric(),
					Unit:       measurement.GetUnit(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteCalculationService/EstimateReviewVote")
		}
	}
	packed, err := anypb.New(req.GetReviewVote())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope reviewsv1.ProductReviewRef
	if err := req.GetReviewVote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	explainReviewVoteCalculationRequest := &reviewsv1.ExplainReviewVoteCalculationRequest{}
	explainReviewVoteCalculationRequest.Context = req.GetContext()
	explainReviewVoteCalculationRequest.ReviewVote = &reviewsv1.ReviewVote{
		Subject: &customersv1.CustomerRef{
			CustomerId: compareReviewVoteOptionsReply.GetReviewVote().GetSubject().GetCustomerId(),
			Email:      compareReviewVoteOptionsReply.GetReviewVote().GetSubject().GetEmail(),
		},
		Detail: packed,
	}
	explainReviewVoteCalculationRequest.Details = &reviewsv1.ExplainReviewVoteCalculationRequest_Details{
		Contact: &commonv1.ContactInfo{
			Email: req.GetDetails().GetContact().GetEmail(),
			Phone: req.GetDetails().GetContact().GetPhone(),
		},
	}
	explainReviewVoteCalculationRequest.ReviewVoteId = envelope.GetDisplayName()
	explainReviewVoteCalculationReply, err := s.reviewVoteCalculationClient.ExplainReviewVoteCalculation(ctx, explainReviewVoteCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteCalculationService/ExplainReviewVoteCalculation")
	}
	_ = explainReviewVoteCalculationReply
	return &reviewsv1.NormalizeReviewVoteResponse{
		Result: &commonv1.OperationResult{
			Details:       compareReviewVoteOptionsReply.GetResult().GetDetails(),
			CorrelationId: compareReviewVoteOptionsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		ReviewVote: &reviewsv1.ReviewVote{
			ReviewVoteId:  req.GetReviewVoteId(),
			CorrelationId: req.GetReviewVote().GetCorrelationId(),
		},
	}, nil
}

// ExplainReviewVoteDecision explains a review vote.
func (s *ReviewVoteValidationServer) ExplainReviewVoteDecision(ctx context.Context, req *reviewsv1.ExplainReviewVoteDecisionRequest) (*reviewsv1.ExplainReviewVoteDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainReviewVoteDecision", "error", err)
		return nil, err
	}
	if err := s.store.SaveReviewVote(ctx, req.GetContext().GetTenantId(), req.GetReviewVote()); err != nil {
		return nil, grpcx.Wrap(err, "save review vote")
	}
	s.log.InfoContext(ctx, "explain review vote decision", "tenant", req.GetContext().GetTenantId())
	var envelope reviewsv1.ProductReviewRef
	if err := req.GetReviewVote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	recalculateSellerPayoutSchedulesRequest := &sellersv1.RecalculateSellerPayoutSchedulesRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					Email:            req.GetDetails().GetContact().GetEmail(),
					PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
					Name:             req.GetDetails().GetContact().GetName(),
				},
			}},
		},
		SellerPayoutScheduleId: envelope.GetDisplayName(),
	}
	recalculateSellerPayoutSchedulesReply, err := recalculateSellerPayoutSchedulesCall(ctx, recalculateSellerPayoutSchedulesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/RecalculateSellerPayoutSchedules")
	}
	return &reviewsv1.ExplainReviewVoteDecisionResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: recalculateSellerPayoutSchedulesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &reviewsv1.ExplainReviewVoteDecisionResponse_Outcome{
			Message: recalculateSellerPayoutSchedulesReply.GetResult().GetMessage(),
		},
		ReviewVote: &reviewsv1.ReviewVote{
			ReviewVoteId: req.GetReviewVoteId(),
			Kind:         req.GetReviewVote().GetKind(),
		},
	}, nil
}

// LintReviewVotes lints a review vote.
func (s *ReviewVoteValidationServer) LintReviewVotes(ctx context.Context, req *reviewsv1.LintReviewVotesRequest) (*reviewsv1.LintReviewVotesResponse, error) {
	if err := s.store.SaveReviewVote(ctx, req.GetContext().GetTenantId(), req.GetReviewVote()); err != nil {
		return nil, grpcx.Wrap(err, "save review vote")
	}
	s.log.InfoContext(ctx, "lint review votes", "tenant", req.GetContext().GetTenantId())
	attributes := make(map[string]string, len(req.GetReviewVote().GetAttributes()))
	for key, value := range req.GetReviewVote().GetAttributes() {
		attributes[key] = value
	}
	var envelope reviewsv1.ProductReviewRef
	if err := req.GetReviewVote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	countAuditEventsRequest := &auditv1.CountAuditEventsRequest{
		Context: req.GetContext(),
		Filter: &auditv1.CountAuditEventsRequest_Filter{
			CreatedAfter: req.GetReviewVote().GetOccurredAt(),
			Labels:       attributes,
			OwnerId:      req.GetReviewVote().GetReviewVoteId(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	var countAuditEventsReply *auditv1.CountAuditEventsResponse
	if err := pipeline.Run(ctx, "acme.audit.v1.AuditEventQueryService/CountAuditEvents", func(ctx context.Context) error {
		answer, err := s.auditEventQueryClient.CountAuditEvents(ctx, countAuditEventsRequest)
		countAuditEventsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventQueryService/CountAuditEvents")
	}
	var envelope2 reviewsv1.ProductReviewRef
	if err := req.GetReviewVote().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	quoteReviewModerationRequest := &reviewsv1.QuoteReviewModerationRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Origin: &reviewsv1.ReviewModeration_Customer{Customer: &customersv1.CustomerRef{Email: req.GetReviewVote().GetSubject().GetEmail()}},
		},
		ReviewModerationId: envelope2.GetDisplayName(),
	}
	quoteReviewModerationReply, err := reviewsv1.ReviewModerationCalculationServiceClient.QuoteReviewModeration(s.reviewModerationCalculationClient, ctx, quoteReviewModerationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationCalculationService/QuoteReviewModeration")
	}
	return &reviewsv1.LintReviewVotesResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: countAuditEventsReply.GetResult().GetCorrelationId(),
			Details:       countAuditEventsReply.GetResult().GetDetails(),
			ReferenceIds:  quoteReviewModerationReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				Email: quoteReviewModerationReply.GetReviewModeration().GetAssignee().GetEmail(),
			},
			ReviewVoteId: req.GetReviewVoteId(),
			Attributes:   req.GetReviewVote().GetAttributes(),
		},
	}, nil
}

// reviewVoteValidationServerScoreProductReviewSteps are the ways to run
// acme.reviews.v1.ProductReviewValidationService/ScoreProductReview, by name.
var reviewVoteValidationServerScoreProductReviewSteps = map[string]func(*ReviewVoteValidationServer, context.Context, *reviewsv1.ScoreProductReviewRequest) (*reviewsv1.ScoreProductReviewResponse, error){
	"default": (*ReviewVoteValidationServer).scoreProductReviewStep,
	"audited": (*ReviewVoteValidationServer).scoreProductReviewAuditedStep,
}

func (s *ReviewVoteValidationServer) scoreProductReviewStep(ctx context.Context, request *reviewsv1.ScoreProductReviewRequest) (*reviewsv1.ScoreProductReviewResponse, error) {
	return s.productReviewValidationClient.ScoreProductReview(ctx, request)
}

func (s *ReviewVoteValidationServer) scoreProductReviewAuditedStep(ctx context.Context, request *reviewsv1.ScoreProductReviewRequest) (*reviewsv1.ScoreProductReviewResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.reviews.v1.ProductReviewValidationService/ScoreProductReview")
	return s.productReviewValidationClient.ScoreProductReview(ctx, request)
}

// reviewVoteValidationServerCheckReviewVoteEligibilityRecalculateReviewVotesStep carries the request of one step of
// CheckReviewVoteEligibility and, once it ran, its reply.
type reviewVoteValidationServerCheckReviewVoteEligibilityRecalculateReviewVotesStep struct {
	request *reviewsv1.RecalculateReviewVotesRequest
	reply   *reviewsv1.RecalculateReviewVotesResponse
}

func (s *ReviewVoteValidationServer) runCheckReviewVoteEligibilityRecalculateReviewVotes(ctx context.Context, step *reviewVoteValidationServerCheckReviewVoteEligibilityRecalculateReviewVotesStep) error {
	reply, err := s.reviewVoteCalculationClient.RecalculateReviewVotes(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
