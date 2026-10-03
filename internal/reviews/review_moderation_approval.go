package reviews

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	expensesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/expenses/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// ReviewModerationApprovalServer implements `acme.reviews.v1.ReviewModerationApprovalService`.
// Routes review moderations through approval, with escalation and delegation.
type ReviewModerationApprovalServer struct {
	reviewsv1.UnimplementedReviewModerationApprovalServiceServer
	store                          *Store
	log                            *slog.Logger
	reviewVoteCalculationClient    reviewsv1.ReviewVoteCalculationServiceClient
	productReviewValidationClient  reviewsv1.ProductReviewValidationServiceClient
	sellerScorecardApprovalClient  sellersv1.SellerScorecardApprovalServiceClient
	ledgerAccountQueryClient       ledgerv1.LedgerAccountQueryServiceClient
	sellerScorecardReportingClient sellersv1.SellerScorecardReportingServiceClient
	expenseReportCalculationClient expensesv1.ExpenseReportCalculationServiceClient
	collectionWorkflowClient       merchandisingv1.CollectionWorkflowServiceClient
	preferenceCalculationPort      preferenceCalculationPort
	metricSeriesClient             metricsv1.MetricSeriesServiceClient
	reviewModerationQueryClient    reviewsv1.ReviewModerationQueryServiceClient
	sellerListingApprovalClient    sellersv1.SellerListingApprovalServiceClient
}

// NewReviewModerationApprovalServer wires a ReviewModerationApprovalServer.
func NewReviewModerationApprovalServer(deps platform.Deps) *ReviewModerationApprovalServer {
	return &ReviewModerationApprovalServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.reviews.v1.ReviewModerationApprovalService"),
		reviewVoteCalculationClient:    reviewsv1.NewReviewVoteCalculationServiceClient(deps.Conn),
		productReviewValidationClient:  reviewsv1.NewProductReviewValidationServiceClient(deps.Conn),
		sellerScorecardApprovalClient:  sellersv1.NewSellerScorecardApprovalServiceClient(deps.Conn),
		ledgerAccountQueryClient:       ledgerv1.NewLedgerAccountQueryServiceClient(deps.Conn),
		sellerScorecardReportingClient: sellersv1.NewSellerScorecardReportingServiceClient(deps.Conn),
		expenseReportCalculationClient: expensesv1.NewExpenseReportCalculationServiceClient(deps.Conn),
		collectionWorkflowClient:       merchandisingv1.NewCollectionWorkflowServiceClient(deps.Conn),
		preferenceCalculationPort:      newPreferenceCalculationPort(deps),
		metricSeriesClient:             metricsv1.NewMetricSeriesServiceClient(deps.Conn),
		reviewModerationQueryClient:    reviewsv1.NewReviewModerationQueryServiceClient(deps.Conn),
		sellerListingApprovalClient:    sellersv1.NewSellerListingApprovalServiceClient(deps.Conn),
	}
}

// SubmitReviewModeration submits a review moderation.
func (s *ReviewModerationApprovalServer) SubmitReviewModeration(ctx context.Context, req *reviewsv1.SubmitReviewModerationRequest) (*reviewsv1.SubmitReviewModerationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReviewModeration(ctx, req.GetContext().GetTenantId(), req.GetReviewModeration()); err != nil {
		return nil, grpcx.Wrap(err, "save review moderation")
	}
	s.log.InfoContext(ctx, "submit review moderation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReviewModeration().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetReviewModeration())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	estimateReviewVoteRequest := &reviewsv1.EstimateReviewVoteRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{Email: req.GetReviewModeration().GetCustomer().GetEmail()},
			Detail:  packed,
		},
	}
	estimateReviewVoteReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*reviewsv1.EstimateReviewVoteResponse, error) {
		return s.reviewVoteCalculationClient.EstimateReviewVote(ctx, estimateReviewVoteRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteCalculationService/EstimateReviewVote")
	}
	var email string
	switch choice := req.GetReviewModeration().GetOrigin().(type) {
	case *reviewsv1.ReviewModeration_Customer:
		email = choice.Customer.GetEmail()
	case *reviewsv1.ReviewModeration_ExternalReference:
		email = choice.ExternalReference
	case *reviewsv1.ReviewModeration_Channel:
		email = choice.Channel.GetChannel()
	}
	packed2, err := anypb.New(req.GetReviewModeration())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	recalculateReviewVotesRequest := &reviewsv1.RecalculateReviewVotesRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{Email: email},
			Detail:  packed2,
		},
	}
	recalculateReviewVotesReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*reviewsv1.RecalculateReviewVotesResponse, error) {
		return s.reviewVoteCalculationClient.RecalculateReviewVotes(ctx, recalculateReviewVotesRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteCalculationService/RecalculateReviewVotes")
	}
	return &reviewsv1.SubmitReviewModerationResponse{
		ReviewModeration: &reviewsv1.ReviewModeration{
			Assignee: &identityv1.UserRef{
				Email: estimateReviewVoteReply.GetReviewVote().GetSubject().GetEmail(),
			},
			ReviewModerationId: req.GetReviewModerationId(),
			Subject:            req.GetReviewModeration().GetSubject(),
		},
		Result: &commonv1.OperationResult{
			Details: recalculateReviewVotesReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// ApproveReviewModeration approves a review moderation.
func (s *ReviewModerationApprovalServer) ApproveReviewModeration(ctx context.Context, req *reviewsv1.ApproveReviewModerationRequest) (*reviewsv1.ApproveReviewModerationResponse, error) {
	if err := s.store.SaveReviewModeration(ctx, req.GetContext().GetTenantId(), req.GetReviewModeration()); err != nil {
		return nil, grpcx.Wrap(err, "save review moderation")
	}
	s.log.InfoContext(ctx, "approve review moderation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReviewModeration().GetAudit().GetEtag())
	validateProductReviewRequest := buildValidateProductReviewRequestForApproveReviewModeration(req)
	validateProductReviewReply, err := s.productReviewValidationClient.ValidateProductReview(ctx, validateProductReviewRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewValidationService/ValidateProductReview")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*sellersv1.EscalateSellerScorecardResponse, len(req.GetReviewModeration().GetComments()))
	for i, comment := range req.GetReviewModeration().GetComments() {
		group.Go(func() error {
			reply, err := s.sellerScorecardApprovalClient.EscalateSellerScorecard(groupCtx, &sellersv1.EscalateSellerScorecardRequest{
				Context: req.GetContext(),
				SellerScorecard: &sellersv1.SellerScorecard{
					Audit: &commonv1.AuditInfo{CreatedAt: comment.GetCreatedAt()},
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
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardApprovalService/EscalateSellerScorecard")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	for _, comment2 := range req.GetReviewModeration().GetComments() {
		if _, err := s.ledgerAccountQueryClient.SuggestLedgerAccounts(ctx, &ledgerv1.SuggestLedgerAccountsRequest{
			Context: req.GetContext(),
			Filter: &ledgerv1.SuggestLedgerAccountsRequest_Filter{
				Region:       validateProductReviewReply.GetProductReview().GetBillingAddress().GetRegion(),
				CreatedAfter: comment2.GetCreatedAt(),
				OwnerId:      comment2.GetCommentId(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountQueryService/SuggestLedgerAccounts")
		}
	}
	return &reviewsv1.ApproveReviewModerationResponse{
		Result: &commonv1.OperationResult{Status: validateProductReviewReply.GetOutcome().GetStatus()},
		ReviewModeration: &reviewsv1.ReviewModeration{
			ReviewModerationId: req.GetReviewModerationId(),
			Assignee:           req.GetReviewModeration().GetAssignee(),
		},
	}, nil
}

// RejectReviewModeration rejects a review moderation.
func (s *ReviewModerationApprovalServer) RejectReviewModeration(ctx context.Context, req *reviewsv1.RejectReviewModerationRequest) (*reviewsv1.RejectReviewModerationResponse, error) {
	if err := s.store.SaveReviewModeration(ctx, req.GetContext().GetTenantId(), req.GetReviewModeration()); err != nil {
		return nil, grpcx.Wrap(err, "save review moderation")
	}
	s.log.InfoContext(ctx, "reject review moderation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReviewModeration().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	generateSellerScorecardReportRequest := &sellersv1.GenerateSellerScorecardReportRequest{}
	generateSellerScorecardReportRequest.Context = req.GetContext()
	generateSellerScorecardReportRequest.Reason = tenancy.Value(ctx, "reason")
	generateSellerScorecardReportRequest.Annotations = annotations
	generateSellerScorecardReportReply, err := s.sellerScorecardReportingClient.GenerateSellerScorecardReport(ctx, generateSellerScorecardReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardReportingService/GenerateSellerScorecardReport")
	}
	rejectSellerScorecardReply, err := s.sellerScorecardApprovalClient.RejectSellerScorecard(ctx, &sellersv1.RejectSellerScorecardRequest{
		Context: req.GetContext(),
		Details: &sellersv1.RejectSellerScorecardRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardApprovalService/RejectSellerScorecard")
	}
	explainExpenseReportCalculationRequest := &expensesv1.ExplainExpenseReportCalculationRequest{
		Context: req.GetContext(),
		ExpenseReport: &expensesv1.ExpenseReport{
			Counterparty: &customersv1.CustomerRef{Email: req.GetReviewModeration().GetCustomer().GetEmail()},
			BillingAddress: &commonv1.PostalAddress{
				Lines: rejectSellerScorecardReply.GetSellerScorecard().GetBillingAddress().GetLines(),
			},
		},
		Details: &expensesv1.ExplainExpenseReportCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: req.GetReviewModeration().GetRequester().GetEmail(),
				Phone: req.GetReviewModeration().GetRequester().GetPhone(),
			},
		},
	}
	type explainExpenseReportCalculationResult struct {
		reply *expensesv1.ExplainExpenseReportCalculationResponse
		err   error
	}
	explainExpenseReportCalculationResults := make(chan explainExpenseReportCalculationResult, 1)
	go func() {
		reply, err := s.expenseReportCalculationClient.ExplainExpenseReportCalculation(ctx, explainExpenseReportCalculationRequest)
		explainExpenseReportCalculationResults <- explainExpenseReportCalculationResult{reply, err}
	}()
	var explainExpenseReportCalculationReply *expensesv1.ExplainExpenseReportCalculationResponse
	select {
	case result := <-explainExpenseReportCalculationResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.expenses.v1.ExpenseReportCalculationService/ExplainExpenseReportCalculation")
		}
		explainExpenseReportCalculationReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &reviewsv1.RejectReviewModerationResponse{
		Result: &commonv1.OperationResult{
			Status:       generateSellerScorecardReportReply.GetResult().GetStatus(),
			Message:      rejectSellerScorecardReply.GetOutcome().GetMessage(),
			ReferenceIds: explainExpenseReportCalculationReply.GetResult().GetReferenceIds(),
		},
		ReviewModeration: &reviewsv1.ReviewModeration{
			ReviewModerationId: req.GetReviewModerationId(),
			Requester:          req.GetReviewModeration().GetRequester(),
		},
	}, nil
}

// EscalateReviewModeration escalates a review moderation.
func (s *ReviewModerationApprovalServer) EscalateReviewModeration(ctx context.Context, req *reviewsv1.EscalateReviewModerationRequest) (*reviewsv1.EscalateReviewModerationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EscalateReviewModeration", "error", err)
		return nil, err
	}
	if err := s.store.SaveReviewModeration(ctx, req.GetContext().GetTenantId(), req.GetReviewModeration()); err != nil {
		return nil, grpcx.Wrap(err, "save review moderation")
	}
	s.log.InfoContext(ctx, "escalate review moderation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReviewModeration().GetAudit().GetEtag())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := reviewModerationApprovalServerRecalculateReviewVotesSteps[mode]
	var email string
	switch choice := req.GetReviewModeration().GetOrigin().(type) {
	case *reviewsv1.ReviewModeration_Customer:
		email = choice.Customer.GetEmail()
	case *reviewsv1.ReviewModeration_ExternalReference:
		email = choice.ExternalReference
	case *reviewsv1.ReviewModeration_Channel:
		email = choice.Channel.GetChannel()
	}
	packed, err := anypb.New(req.GetReviewModeration())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	recalculateReviewVotesRequest := &reviewsv1.RecalculateReviewVotesRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
				Email:       email,
				CustomerId:  req.GetReviewModeration().GetCustomer().GetCustomerId(),
			},
			Detail: packed,
		},
		Annotations: req.GetAnnotations(),
	}
	recalculateReviewVotesReply, err := run(s, ctx, recalculateReviewVotesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteCalculationService/RecalculateReviewVotes")
	}
	return &reviewsv1.EscalateReviewModerationResponse{
		ReviewModeration: &reviewsv1.ReviewModeration{
			Requester: &commonv1.ContactInfo{
				Email: recalculateReviewVotesReply.GetReviewVote().GetSubject().GetEmail(),
			},
			ReviewModerationId: req.GetReviewModerationId(),
			Subject:            req.GetReviewModeration().GetSubject(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListPendingReviewModerations lists review moderations a page at a time.
func (s *ReviewModerationApprovalServer) ListPendingReviewModerations(ctx context.Context, req *reviewsv1.ListPendingReviewModerationsRequest) (*reviewsv1.ListPendingReviewModerationsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending review moderations", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "user_id", req.GetContext().GetActor().GetUserId())
	pauseCollectionWorkflowRequest := &merchandisingv1.PauseCollectionWorkflowRequest{
		Context: req.GetContext(),
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
				}},
				Manager: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
			}},
			Manager: &identityv1.UserRef{UserId: tenancy.Value(ctx, "user_id")},
		},
	}
	pauseCollectionWorkflowReply, err := grpcx.Call(ctx, s.collectionWorkflowClient.PauseCollectionWorkflow, pauseCollectionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionWorkflowService/PauseCollectionWorkflow")
	}
	return &reviewsv1.ListPendingReviewModerationsResponse{
		ReviewModerations: []*reviewsv1.ReviewModeration{&reviewsv1.ReviewModeration{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: pauseCollectionWorkflowReply.GetCollection().GetAudit().GetUpdatedBy(),
			},
		}},
		Result: &commonv1.OperationResult{
			ReferenceIds: pauseCollectionWorkflowReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// RecallReviewModeration recalls a review moderation.
func (s *ReviewModerationApprovalServer) RecallReviewModeration(ctx context.Context, req *reviewsv1.RecallReviewModerationRequest) (*reviewsv1.RecallReviewModerationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReviewModeration(ctx, req.GetContext().GetTenantId(), req.GetReviewModeration()); err != nil {
		return nil, grpcx.Wrap(err, "save review moderation")
	}
	s.log.InfoContext(ctx, "recall review moderation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReviewModeration().GetAudit().GetEtag())
	reviewModerationCache := cache.New[string, *reviewsv1.ReviewModeration](1)
	reviewModerationCache.Put(req.GetContext().GetTraceId(), req.GetReviewModeration())
	cachedReviewModeration, _ := reviewModerationCache.Get(req.GetContext().GetTraceId())
	ctx = tenancy.With(ctx, "display_name", req.GetReviewModeration().GetAssignee().GetDisplayName())
	var holder struct{ UserId string }
	if record := req.GetReviewModeration().GetAssignee(); record != nil {
		if err := mapper.CopyFields(&holder, record, "UserId"); err != nil {
			s.log.WarnContext(ctx, "copy user_id", "error", err)
		}
	}
	calculatePreferenceRequest := &customersv1.CalculatePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{CreatedAt: cachedReviewModeration.GetAudit().GetCreatedAt()},
			Owner: &identityv1.UserRef{
				DisplayName: tenancy.Value(ctx, "display_name"),
				UserId:      holder.UserId,
				Email:       req.GetDetails().GetApprover().GetEmail(),
			},
		},
	}
	calculatePreferenceReply, err := s.preferenceCalculationPort.CalculatePreference(ctx, calculatePreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/CalculatePreference")
	}
	return &reviewsv1.RecallReviewModerationResponse{
		ReviewModeration: &reviewsv1.ReviewModeration{
			Priority:           calculatePreferenceReply.GetPreference().GetPriority(),
			ReviewModerationId: req.GetReviewModerationId(),
		},
		Outcome: &reviewsv1.RecallReviewModerationResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetReviewModerationApprovalStatus returns a review moderation.
func (s *ReviewModerationApprovalServer) GetReviewModerationApprovalStatus(ctx context.Context, req *reviewsv1.GetReviewModerationApprovalStatusRequest) (*reviewsv1.GetReviewModerationApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetReviewModerationApprovalStatus", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "review_moderation", req.GetReviewModerationId(), "get_review_moderation_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get review moderation approval status", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	listMetricSeriesesRequest := &metricsv1.ListMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter: &metricsv1.ListMetricSeriesesRequest_Filter{
			OwnerId:      req.GetReviewModerationId(),
			CreatedAfter: req.GetContext().GetDeadline(),
			Labels:       baggage,
		},
	}
	var listMetricSeriesesReply *metricsv1.ListMetricSeriesesResponse
	if err := pipeline.Run(ctx, "acme.metrics.v1.MetricSeriesService/ListMetricSerieses", func(ctx context.Context) error {
		answer, err := s.metricSeriesClient.ListMetricSerieses(ctx, listMetricSeriesesRequest)
		listMetricSeriesesReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesService/ListMetricSerieses")
	}
	return &reviewsv1.GetReviewModerationApprovalStatusResponse{
		Result: &commonv1.OperationResult{
			Message: listMetricSeriesesReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		ReviewModeration: &reviewsv1.ReviewModeration{ReviewModerationId: req.GetReviewModerationId()},
	}, nil
}

// CommentReviewModerationApproval comments a review moderation.
func (s *ReviewModerationApprovalServer) CommentReviewModerationApproval(ctx context.Context, req *reviewsv1.CommentReviewModerationApprovalRequest) (*reviewsv1.CommentReviewModerationApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReviewModeration(ctx, req.GetContext().GetTenantId(), req.GetReviewModeration()); err != nil {
		return nil, grpcx.Wrap(err, "save review moderation")
	}
	s.log.InfoContext(ctx, "comment review moderation approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReviewModeration().GetAudit().GetEtag())
	peekReviewModerationRequest := buildPeekReviewModerationRequestForCommentReviewModerationApproval(req)
	peekReviewModerationReply, err := s.reviewModerationQueryClient.PeekReviewModeration(ctx, peekReviewModerationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationQueryService/PeekReviewModeration")
	}
	submitSellerListingRequest := &sellersv1.SubmitSellerListingRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetReviewModeration().GetAudit().GetCreatedBy(),
			},
			Owner: &identityv1.UserRef{
				Email: peekReviewModerationReply.GetReviewModeration().GetCustomer().GetEmail(),
			},
		},
	}
	submitSellerListingReply, err := sellersv1.SellerListingApprovalServiceClient.SubmitSellerListing(s.sellerListingApprovalClient, ctx, submitSellerListingRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingApprovalService/SubmitSellerListing")
	}
	_ = submitSellerListingReply
	return &reviewsv1.CommentReviewModerationApprovalResponse{
		Result: &commonv1.OperationResult{Status: peekReviewModerationReply.GetResult().GetStatus()},
		ReviewModeration: &reviewsv1.ReviewModeration{
			Audit: &commonv1.AuditInfo{
				CreatedBy: peekReviewModerationReply.GetReviewModeration().GetAudit().GetCreatedBy(),
			},
			ReviewModerationId: req.GetReviewModerationId(),
			Requester:          req.GetReviewModeration().GetRequester(),
		},
	}, nil
}

// reviewModerationApprovalServerRecalculateReviewVotesSteps are the ways to run
// acme.reviews.v1.ReviewVoteCalculationService/RecalculateReviewVotes, by name.
var reviewModerationApprovalServerRecalculateReviewVotesSteps = map[string]func(*ReviewModerationApprovalServer, context.Context, *reviewsv1.RecalculateReviewVotesRequest) (*reviewsv1.RecalculateReviewVotesResponse, error){
	"default": (*ReviewModerationApprovalServer).recalculateReviewVotesStep,
	"audited": (*ReviewModerationApprovalServer).recalculateReviewVotesAuditedStep,
}

func (s *ReviewModerationApprovalServer) recalculateReviewVotesStep(ctx context.Context, request *reviewsv1.RecalculateReviewVotesRequest) (*reviewsv1.RecalculateReviewVotesResponse, error) {
	return s.reviewVoteCalculationClient.RecalculateReviewVotes(ctx, request)
}

func (s *ReviewModerationApprovalServer) recalculateReviewVotesAuditedStep(ctx context.Context, request *reviewsv1.RecalculateReviewVotesRequest) (*reviewsv1.RecalculateReviewVotesResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.reviews.v1.ReviewVoteCalculationService/RecalculateReviewVotes")
	return s.reviewVoteCalculationClient.RecalculateReviewVotes(ctx, request)
}
