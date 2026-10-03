package reviews

import (
	"context"
	"log/slog"

	appointmentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/appointments/v1"
	bankingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/banking/v1"
	careplansv1 "github.com/hounddogai/protomap-test-golang/gen/acme/careplans/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ReviewVoteCalculationServer implements `acme.reviews.v1.ReviewVoteCalculationService`.
// Computes the figures of a review vote, such as totals, taxes, and estimates.
type ReviewVoteCalculationServer struct {
	reviewsv1.UnimplementedReviewVoteCalculationServiceServer
	store                              *Store
	log                                *slog.Logger
	clients                            *registry.Registry
	explainMerchandisingRuleDecisionFn func(context.Context, *merchandisingv1.ExplainMerchandisingRuleDecisionRequest, ...grpc.CallOption) (*merchandisingv1.ExplainMerchandisingRuleDecisionResponse, error)
	reviewModerationCalculationClient  reviewsv1.ReviewModerationCalculationServiceClient
	conn                               *grpc.ClientConn
	ledgerAccountClient                ledgerv1.LedgerAccountServiceClient
	meteredSynonymExport               meteredSynonymExport
	bankAccountQueryClient             bankingv1.BankAccountQueryServiceClient
	carePlanCalculationClient          careplansv1.CarePlanCalculationServiceClient
	productReviewValidationClient      reviewsv1.ProductReviewValidationServiceClient
	sellerClient                       sellersv1.SellerServiceClient
}

// NewReviewVoteCalculationServer wires a ReviewVoteCalculationServer.
func NewReviewVoteCalculationServer(deps platform.Deps) *ReviewVoteCalculationServer {
	merchandisingRuleValidation := merchandisingv1.NewMerchandisingRuleValidationServiceClient(deps.Conn)
	return &ReviewVoteCalculationServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.reviews.v1.ReviewVoteCalculationService"),
		clients:                            deps.Clients,
		explainMerchandisingRuleDecisionFn: merchandisingRuleValidation.ExplainMerchandisingRuleDecision,
		reviewModerationCalculationClient:  reviewsv1.NewReviewModerationCalculationServiceClient(deps.Conn),
		conn:                               deps.Conn,
		ledgerAccountClient:                ledgerv1.NewLedgerAccountServiceClient(deps.Conn),
		meteredSynonymExport:               meteredSynonymExport{SynonymExportServiceClient: searchv1.NewSynonymExportServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		bankAccountQueryClient:             bankingv1.NewBankAccountQueryServiceClient(deps.Conn),
		carePlanCalculationClient:          careplansv1.NewCarePlanCalculationServiceClient(deps.Conn),
		productReviewValidationClient:      reviewsv1.NewProductReviewValidationServiceClient(deps.Conn),
		sellerClient:                       sellersv1.NewSellerServiceClient(deps.Conn),
	}
}

// CalculateReviewVote calculates a review vote.
func (s *ReviewVoteCalculationServer) CalculateReviewVote(ctx context.Context, req *reviewsv1.CalculateReviewVoteRequest) (*reviewsv1.CalculateReviewVoteResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CalculateReviewVote", "error", err)
		return nil, err
	}
	if err := s.store.SaveReviewVote(ctx, req.GetContext().GetTenantId(), req.GetReviewVote()); err != nil {
		return nil, grpcx.Wrap(err, "save review vote")
	}
	s.log.InfoContext(ctx, "calculate review vote", "tenant", req.GetContext().GetTenantId())
	sellerPayoutScheduleValidation, err := registry.Lookup[sellersv1.SellerPayoutScheduleValidationServiceClient](s.clients, "acme.sellers.v1.SellerPayoutScheduleValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	source := req.GetReviewVote().GetSubject().ProtoReflect()
	displayName := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("display_name"))).String()
	var envelope reviewsv1.ProductReviewRef
	if err := req.GetReviewVote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	explainSellerPayoutScheduleDecisionRequest := &sellersv1.ExplainSellerPayoutScheduleDecisionRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Organizer: &identityv1.UserRef{
				Email:       req.GetReviewVote().GetSubject().GetEmail(),
				DisplayName: displayName,
			},
			Location: &commonv1.PostalAddress{
				Recipient: req.GetDetails().GetDestination().GetRecipient(),
				Locality:  req.GetDetails().GetDestination().GetLocality(),
			},
		},
		SellerPayoutScheduleId: envelope.GetDisplayName(),
	}
	explainSellerPayoutScheduleDecisionReply, err := sellerPayoutScheduleValidation.ExplainSellerPayoutScheduleDecision(ctx, explainSellerPayoutScheduleDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleValidationService/ExplainSellerPayoutScheduleDecision")
	}
	ctx = tenancy.With(ctx, "email", req.GetReviewVote().GetSubject().GetEmail())
	var envelope2 reviewsv1.ProductReviewRef
	if err := req.GetReviewVote().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	explainMerchandisingRuleDecisionRequest := &merchandisingv1.ExplainMerchandisingRuleDecisionRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Audit: &commonv1.AuditInfo{
				Etag: explainSellerPayoutScheduleDecisionReply.GetSellerPayoutSchedule().GetAudit().GetEtag(),
			},
			Owner: &identityv1.UserRef{Email: tenancy.Value(ctx, "email")},
		},
		MerchandisingRuleId: envelope2.GetDisplayName(),
	}
	explainMerchandisingRuleDecisionReply, err := s.explainMerchandisingRuleDecisionFn(ctx, explainMerchandisingRuleDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleValidationService/ExplainMerchandisingRuleDecision")
	}
	return &reviewsv1.CalculateReviewVoteResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: explainSellerPayoutScheduleDecisionReply.GetResult().GetCorrelationId(),
			ReferenceIds:  explainMerchandisingRuleDecisionReply.GetResult().GetReferenceIds(),
			Message:       explainMerchandisingRuleDecisionReply.GetOutcome().GetMessage(),
			Status:        "ok",
		},
		ReviewVote: &reviewsv1.ReviewVote{
			ReviewVoteId: req.GetReviewVoteId(),
			Subject:      req.GetReviewVote().GetSubject(),
		},
	}, nil
}

// EstimateReviewVote estimates a review vote.
func (s *ReviewVoteCalculationServer) EstimateReviewVote(ctx context.Context, req *reviewsv1.EstimateReviewVoteRequest) (*reviewsv1.EstimateReviewVoteResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReviewVote(ctx, req.GetContext().GetTenantId(), req.GetReviewVote()); err != nil {
		return nil, grpcx.Wrap(err, "save review vote")
	}
	s.log.InfoContext(ctx, "estimate review vote", "tenant", req.GetContext().GetTenantId())
	for _, measurement := range req.GetReviewVote().GetMeasurements() {
		if _, err := s.reviewModerationCalculationClient.EstimateReviewModeration(ctx, &reviewsv1.EstimateReviewModerationRequest{
			Context: req.GetContext(),
			Details: &reviewsv1.EstimateReviewModerationRequest_Details{
				Destination: &commonv1.PostalAddress{Locality: req.GetDetails().GetDestination().GetLocality()},
			},
			ReviewModeration: &reviewsv1.ReviewModeration{
				Origin: &reviewsv1.ReviewModeration_Customer{Customer: &customersv1.CustomerRef{Email: req.GetReviewVote().GetSubject().GetEmail()}},
				Sla:    &reviewsv1.ReviewModerationSla{DueAt: measurement.GetObservedAt()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationCalculationService/EstimateReviewModeration")
		}
	}
	var envelope reviewsv1.ProductReviewRef
	if err := req.GetReviewVote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	recalculateReviewModerationsRequest := &reviewsv1.RecalculateReviewModerationsRequest{
		Context:            req.GetContext(),
		ReviewModerationId: envelope.GetDisplayName(),
	}
	recalculateReviewModerationsReply := new(reviewsv1.RecalculateReviewModerationsResponse)
	if err := grpcx.Invoke(ctx, s.conn, reviewsv1.ReviewModerationCalculationService_RecalculateReviewModerations_FullMethodName, recalculateReviewModerationsRequest, recalculateReviewModerationsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationCalculationService/RecalculateReviewModerations")
	}
	encoded, err := protojson.Marshal(req.GetReviewVote())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded reviewsv1.ReviewVote
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	var envelope2 reviewsv1.ProductReviewRef
	if err := req.GetReviewVote().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	quoteReviewModerationRequest := &reviewsv1.QuoteReviewModerationRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Requester: &commonv1.ContactInfo{Email: (&decoded).GetSubject().GetEmail()},
		},
		ReviewModerationId: envelope2.GetDisplayName(),
	}
	quoteReviewModerationReply := new(reviewsv1.QuoteReviewModerationResponse)
	if err := grpcx.Invoke(ctx, s.conn, reviewsv1.ReviewModerationCalculationService_QuoteReviewModeration_FullMethodName, quoteReviewModerationRequest, quoteReviewModerationReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationCalculationService/QuoteReviewModeration")
	}
	return &reviewsv1.EstimateReviewVoteResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: recalculateReviewModerationsReply.GetResult().GetCorrelationId(),
			Status:        quoteReviewModerationReply.GetResult().GetStatus(),
		},
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				DisplayName: quoteReviewModerationReply.GetReviewModeration().GetCustomer().GetDisplayName(),
			},
			ReviewVoteId: req.GetReviewVoteId(),
			Source:       req.GetReviewVote().GetSource(),
		},
	}, nil
}

// QuoteReviewVote quotes a review vote.
func (s *ReviewVoteCalculationServer) QuoteReviewVote(ctx context.Context, req *reviewsv1.QuoteReviewVoteRequest) (*reviewsv1.QuoteReviewVoteResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReviewVote(ctx, req.GetContext().GetTenantId(), req.GetReviewVote()); err != nil {
		return nil, grpcx.Wrap(err, "save review vote")
	}
	s.log.InfoContext(ctx, "quote review vote", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*reviewsv1.QuoteReviewVoteRequest_Details)
	detailsCache := cache.New[string, *reviewsv1.QuoteReviewVoteRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	calculateReviewModerationRequest := &reviewsv1.CalculateReviewModerationRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Assignee: &identityv1.UserRef{
				DisplayName: req.GetReviewVote().GetSubject().GetDisplayName(),
				Email:       req.GetReviewVote().GetSubject().GetEmail(),
			},
		},
		Details: &reviewsv1.CalculateReviewModerationRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: cloned.GetDestination().GetCountryCode(),
				Lines:       cachedDetails.GetDestination().GetLines(),
			},
		},
	}
	calculateReviewModerationReply := new(reviewsv1.CalculateReviewModerationResponse)
	if err := s.conn.Invoke(ctx, reviewsReviewModerationCalculationServiceCalculateReviewModerationMethod, calculateReviewModerationRequest, calculateReviewModerationReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationCalculationService/CalculateReviewModeration")
	}
	return &reviewsv1.QuoteReviewVoteResponse{
		Result: &commonv1.OperationResult{
			Details: calculateReviewModerationReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		ReviewVote: &reviewsv1.ReviewVote{
			ReviewVoteId: req.GetReviewVoteId(),
			Subject:      req.GetReviewVote().GetSubject(),
		},
	}, nil
}

// CompareReviewVoteOptions compares a review vote.
func (s *ReviewVoteCalculationServer) CompareReviewVoteOptions(ctx context.Context, req *reviewsv1.CompareReviewVoteOptionsRequest) (*reviewsv1.CompareReviewVoteOptionsResponse, error) {
	if err := s.store.SaveReviewVote(ctx, req.GetContext().GetTenantId(), req.GetReviewVote()); err != nil {
		return nil, grpcx.Wrap(err, "save review vote")
	}
	s.log.InfoContext(ctx, "compare review vote options", "tenant", req.GetContext().GetTenantId())
	for measurement := range iterx.Each(req.GetReviewVote().GetMeasurements(), func(candidate *reviewsv1.ReviewVoteMeasurement) bool { return candidate != nil }) {
		if _, err := s.ledgerAccountClient.SearchLedgerAccounts(ctx, &ledgerv1.SearchLedgerAccountsRequest{
			Context: req.GetContext(),
			Filter: &ledgerv1.SearchLedgerAccountsRequest_Filter{
				CreatedAfter: measurement.GetObservedAt(),
				Region:       req.GetDetails().GetDestination().GetRegion(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/SearchLedgerAccounts")
		}
	}
	for _, measurement2 := range req.GetReviewVote().GetMeasurements() {
		if _, err := s.reviewModerationCalculationClient.EstimateReviewModeration(ctx, &reviewsv1.EstimateReviewModerationRequest{
			Context: req.GetContext(),
			ReviewModeration: &reviewsv1.ReviewModeration{
				Sla: &reviewsv1.ReviewModerationSla{DueAt: measurement2.GetObservedAt()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationCalculationService/EstimateReviewModeration")
		}
	}
	return &reviewsv1.CompareReviewVoteOptionsResponse{
		ReviewVote: &reviewsv1.ReviewVote{
			ReviewVoteId: req.GetReviewVoteId(),
			Attributes:   req.GetReviewVote().GetAttributes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecalculateReviewVotes recalculates a review vote.
func (s *ReviewVoteCalculationServer) RecalculateReviewVotes(ctx context.Context, req *reviewsv1.RecalculateReviewVotesRequest) (*reviewsv1.RecalculateReviewVotesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReviewVote(ctx, req.GetContext().GetTenantId(), req.GetReviewVote()); err != nil {
		return nil, grpcx.Wrap(err, "save review vote")
	}
	s.log.InfoContext(ctx, "recalculate review votes", "tenant", req.GetContext().GetTenantId())
	scheduleSynonymExportRequest := &searchv1.ScheduleSynonymExportRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: req.GetAnnotations(),
		Details:     &searchv1.ScheduleSynonymExportRequest_Details{Filters: req.GetReviewVote().GetAttributes()},
	}
	scheduleSynonymExportReply, err := s.meteredSynonymExport.ScheduleSynonymExport(ctx, scheduleSynonymExportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/ScheduleSynonymExport")
	}
	var envelope reviewsv1.ProductReviewRef
	if err := req.GetReviewVote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	suggestBankAccountsRequest := &bankingv1.SuggestBankAccountsRequest{
		Context: req.GetContext(),
		Filter: &bankingv1.SuggestBankAccountsRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: req.GetReviewVote().GetOccurredAt(),
			OwnerId:      req.GetReviewVote().GetCorrelationId(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	suggestBankAccountsReply, err := grpcx.Call(ctx, s.bankAccountQueryClient.SuggestBankAccounts, suggestBankAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.banking.v1.BankAccountQueryService/SuggestBankAccounts")
	}
	return &reviewsv1.RecalculateReviewVotesResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: scheduleSynonymExportReply.GetResult().GetReferenceIds(),
			Status:       suggestBankAccountsReply.GetResult().GetStatus(),
		},
		ReviewVote: &reviewsv1.ReviewVote{
			ReviewVoteId: req.GetReviewVoteId(),
			Sequence:     req.GetReviewVote().GetSequence(),
		},
	}, nil
}

// ExplainReviewVoteCalculation explains a review vote.
func (s *ReviewVoteCalculationServer) ExplainReviewVoteCalculation(ctx context.Context, req *reviewsv1.ExplainReviewVoteCalculationRequest) (*reviewsv1.ExplainReviewVoteCalculationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReviewVote(ctx, req.GetContext().GetTenantId(), req.GetReviewVote()); err != nil {
		return nil, grpcx.Wrap(err, "save review vote")
	}
	s.log.InfoContext(ctx, "explain review vote calculation", "tenant", req.GetContext().GetTenantId())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*careplansv1.QuoteCarePlanResponse, len(req.GetReviewVote().GetMeasurements()))
	for i, measurement := range req.GetReviewVote().GetMeasurements() {
		group.Go(func() error {
			reply, err := s.carePlanCalculationClient.QuoteCarePlan(groupCtx, &careplansv1.QuoteCarePlanRequest{
				Context: req.GetContext(),
				CarePlan: &careplansv1.CarePlan{
					Audit: &commonv1.AuditInfo{CreatedAt: measurement.GetObservedAt()},
					Counterparty: &appointmentsv1.AppointmentRef{
						DisplayName: req.GetReviewVote().GetSubject().GetDisplayName(),
					},
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
		return nil, grpcx.Wrap(err, "acme.careplans.v1.CarePlanCalculationService/QuoteCarePlan")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	previewProductReviewReply, err := s.productReviewValidationClient.PreviewProductReview(ctx, &reviewsv1.PreviewProductReviewRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.PreviewProductReviewRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetContact().GetName()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewValidationService/PreviewProductReview")
	}
	var envelope reviewsv1.ProductReviewRef
	if err := req.GetReviewVote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	calculateReviewModerationRequest := &reviewsv1.CalculateReviewModerationRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: previewProductReviewReply.GetProductReview().GetAudit().GetUpdatedBy(),
			},
			Origin:   &reviewsv1.ReviewModeration_Customer{Customer: &customersv1.CustomerRef{Email: req.GetReviewVote().GetSubject().GetEmail()}},
			Assignee: &identityv1.UserRef{Email: req.GetDetails().GetContact().GetEmail()},
		},
		ReviewModerationId: envelope.GetDisplayName(),
	}
	calculateReviewModerationReply := new(reviewsv1.CalculateReviewModerationResponse)
	if err := s.conn.Invoke(ctx, reviewsReviewModerationCalculationServiceCalculateReviewModerationMethod, calculateReviewModerationRequest, calculateReviewModerationReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationCalculationService/CalculateReviewModeration")
	}
	return &reviewsv1.ExplainReviewVoteCalculationResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: previewProductReviewReply.GetResult().GetReferenceIds(),
			Message:      calculateReviewModerationReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				DisplayName: calculateReviewModerationReply.GetReviewModeration().GetAssignee().GetDisplayName(),
			},
			ReviewVoteId: req.GetReviewVoteId(),
			Source:       req.GetReviewVote().GetSource(),
		},
	}, nil
}

// GetReviewVoteRates returns a review vote.
func (s *ReviewVoteCalculationServer) GetReviewVoteRates(ctx context.Context, req *reviewsv1.GetReviewVoteRatesRequest) (*reviewsv1.GetReviewVoteRatesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "review_vote", req.GetReviewVoteId(), "get_review_vote_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get review vote rates", "tenant", req.GetContext().GetTenantId())
	createSellerRequest := &sellersv1.CreateSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
	}
	createSellerReply, err := sellersv1.SellerServiceClient.CreateSeller(s.sellerClient, ctx, createSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerService/CreateSeller")
	}
	return &reviewsv1.GetReviewVoteRatesResponse{
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				Email: createSellerReply.GetSeller().GetContact().GetEmail(),
			},
			CorrelationId: createSellerReply.GetResult().GetCorrelationId(),
			ReviewVoteId:  req.GetReviewVoteId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
