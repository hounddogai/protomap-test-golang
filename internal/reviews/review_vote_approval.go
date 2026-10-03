package reviews

import (
	"context"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	maintenancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/maintenance/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// ReviewVoteApprovalServer implements `acme.reviews.v1.ReviewVoteApprovalService`.
// Routes review votes through approval, with escalation and delegation.
type ReviewVoteApprovalServer struct {
	reviewsv1.UnimplementedReviewVoteApprovalServiceServer
	store                             *Store
	log                               *slog.Logger
	preferenceValidationClient        customersv1.PreferenceValidationServiceClient
	reviewModerationQueryClient       reviewsv1.ReviewModerationQueryServiceClient
	conn                              *grpc.ClientConn
	giftCardTransactionApprovalClient giftcardsv1.GiftCardTransactionApprovalServiceClient
	searchIndexHistoryClient          searchv1.SearchIndexHistoryServiceClient
	sellerWorkflowClient              sellersv1.SellerWorkflowServiceClient
	sellerListingWorkflowClient       sellersv1.SellerListingWorkflowServiceClient
	workOrderQueryClient              maintenancev1.WorkOrderQueryServiceClient
	productReviewApprovalClient       reviewsv1.ProductReviewApprovalServiceClient
	giftCardBatchWorkflowClient       giftcardsv1.GiftCardBatchWorkflowServiceClient
	clients                           *registry.Registry
	tenantAdminClient                 identityv1.TenantAdminServiceClient
	householdQueryOnce                sync.Once
	householdQueryCached              customersv1.HouseholdQueryServiceClient
}

// NewReviewVoteApprovalServer wires a ReviewVoteApprovalServer.
func NewReviewVoteApprovalServer(deps platform.Deps) *ReviewVoteApprovalServer {
	return &ReviewVoteApprovalServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.reviews.v1.ReviewVoteApprovalService"),
		preferenceValidationClient:        customersv1.NewPreferenceValidationServiceClient(deps.Conn),
		reviewModerationQueryClient:       reviewsv1.NewReviewModerationQueryServiceClient(deps.Conn),
		conn:                              deps.Conn,
		giftCardTransactionApprovalClient: giftcardsv1.NewGiftCardTransactionApprovalServiceClient(deps.Conn),
		searchIndexHistoryClient:          searchv1.NewSearchIndexHistoryServiceClient(deps.Conn),
		sellerWorkflowClient:              sellersv1.NewSellerWorkflowServiceClient(deps.Conn),
		sellerListingWorkflowClient:       sellersv1.NewSellerListingWorkflowServiceClient(deps.Conn),
		workOrderQueryClient:              maintenancev1.NewWorkOrderQueryServiceClient(deps.Conn),
		productReviewApprovalClient:       reviewsv1.NewProductReviewApprovalServiceClient(deps.Conn),
		giftCardBatchWorkflowClient:       giftcardsv1.NewGiftCardBatchWorkflowServiceClient(deps.Conn),
		clients:                           deps.Clients,
		tenantAdminClient:                 identityv1.NewTenantAdminServiceClient(deps.Conn),
	}
}

// SubmitReviewVote submits a review vote.
func (s *ReviewVoteApprovalServer) SubmitReviewVote(ctx context.Context, req *reviewsv1.SubmitReviewVoteRequest) (*reviewsv1.SubmitReviewVoteResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubmitReviewVote", "error", err)
		return nil, err
	}
	if err := s.store.SaveReviewVote(ctx, req.GetContext().GetTenantId(), req.GetReviewVote()); err != nil {
		return nil, grpcx.Wrap(err, "save review vote")
	}
	s.log.InfoContext(ctx, "submit review vote", "tenant", req.GetContext().GetTenantId())
	var envelope reviewsv1.ProductReviewRef
	if err := req.GetReviewVote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	checkPreferenceEligibilityReply, err := s.preferenceValidationClient.CheckPreferenceEligibility(ctx, &customersv1.CheckPreferenceEligibilityRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				Email:       req.GetReviewVote().GetSubject().GetEmail(),
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
		Details: &customersv1.CheckPreferenceEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
		PreferenceId: envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/CheckPreferenceEligibility")
	}
	return &reviewsv1.SubmitReviewVoteResponse{
		Result: &commonv1.OperationResult{
			Status:        checkPreferenceEligibilityReply.GetOutcome().GetStatus(),
			CorrelationId: req.GetReviewVote().GetCorrelationId(),
		},
		Outcome: &reviewsv1.SubmitReviewVoteResponse_Outcome{
			EffectiveAt: checkPreferenceEligibilityReply.GetOutcome().GetEffectiveAt(),
		},
		ReviewVote: &reviewsv1.ReviewVote{ReviewVoteId: req.GetReviewVoteId()},
	}, nil
}

// ApproveReviewVote approves a review vote.
func (s *ReviewVoteApprovalServer) ApproveReviewVote(ctx context.Context, req *reviewsv1.ApproveReviewVoteRequest) (*reviewsv1.ApproveReviewVoteResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ApproveReviewVote", "error", err)
		return nil, err
	}
	if err := s.store.SaveReviewVote(ctx, req.GetContext().GetTenantId(), req.GetReviewVote()); err != nil {
		return nil, grpcx.Wrap(err, "save review vote")
	}
	s.log.InfoContext(ctx, "approve review vote", "tenant", req.GetContext().GetTenantId())
	resolveReviewModerationsReply, err := s.reviewModerationQueryClient.ResolveReviewModerations(ctx, &reviewsv1.ResolveReviewModerationsRequest{Context: req.GetContext()})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationQueryService/ResolveReviewModerations")
	}
	batchGetFeatureFlagsRequest := &featureflagsv1.BatchGetFeatureFlagsRequest{Context: req.GetContext()}
	batchGetFeatureFlagsReply := new(featureflagsv1.BatchGetFeatureFlagsResponse)
	if err := grpcx.Invoke(ctx, s.conn, featureflagsv1.FeatureFlagService_BatchGetFeatureFlags_FullMethodName, batchGetFeatureFlagsRequest, batchGetFeatureFlagsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagService/BatchGetFeatureFlags")
	}
	return &reviewsv1.ApproveReviewVoteResponse{
		Outcome: &reviewsv1.ApproveReviewVoteResponse_Outcome{
			Status: resolveReviewModerationsReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Message:      resolveReviewModerationsReply.GetResult().GetMessage(),
			ReferenceIds: batchGetFeatureFlagsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		ReviewVote: &reviewsv1.ReviewVote{
			ReviewVoteId: req.GetReviewVoteId(),
			Kind:         req.GetReviewVote().GetKind(),
		},
	}, nil
}

// RejectReviewVote rejects a review vote.
func (s *ReviewVoteApprovalServer) RejectReviewVote(ctx context.Context, req *reviewsv1.RejectReviewVoteRequest) (*reviewsv1.RejectReviewVoteResponse, error) {
	if err := s.store.SaveReviewVote(ctx, req.GetContext().GetTenantId(), req.GetReviewVote()); err != nil {
		return nil, grpcx.Wrap(err, "save review vote")
	}
	s.log.InfoContext(ctx, "reject review vote", "tenant", req.GetContext().GetTenantId())
	escalateGiftCardTransactionRequest := buildEscalateGiftCardTransactionRequestForRejectReviewVote(req)
	escalateGiftCardTransactionReply, err := s.giftCardTransactionApprovalClient.EscalateGiftCardTransaction(ctx, escalateGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionApprovalService/EscalateGiftCardTransaction")
	}
	for _, measurement := range req.GetReviewVote().GetMeasurements() {
		if _, err := s.searchIndexHistoryClient.ListSearchIndexVersions(ctx, &searchv1.ListSearchIndexVersionsRequest{
			Context: req.GetContext(),
			Filter: &searchv1.ListSearchIndexVersionsRequest_Filter{
				CreatedAfter: measurement.GetObservedAt(),
				OwnerId:      escalateGiftCardTransactionReply.GetOutcome().GetReferenceId(),
				Labels:       escalateGiftCardTransactionReply.GetGiftCardTransaction().GetAttributes(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexHistoryService/ListSearchIndexVersions")
		}
	}
	listSellerWorkflowsRequest := buildListSellerWorkflowsRequestForRejectReviewVote(req, escalateGiftCardTransactionReply)
	listSellerWorkflowsReply, err := s.sellerWorkflowClient.ListSellerWorkflows(ctx, listSellerWorkflowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerWorkflowService/ListSellerWorkflows")
	}
	return &reviewsv1.RejectReviewVoteResponse{
		Outcome: &reviewsv1.RejectReviewVoteResponse_Outcome{
			Message: escalateGiftCardTransactionReply.GetOutcome().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			Details: listSellerWorkflowsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		ReviewVote: &reviewsv1.ReviewVote{
			ReviewVoteId: req.GetReviewVoteId(),
			OccurredAt:   req.GetReviewVote().GetOccurredAt(),
		},
	}, nil
}

// EscalateReviewVote escalates a review vote.
func (s *ReviewVoteApprovalServer) EscalateReviewVote(ctx context.Context, req *reviewsv1.EscalateReviewVoteRequest) (*reviewsv1.EscalateReviewVoteResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReviewVote(ctx, req.GetContext().GetTenantId(), req.GetReviewVote()); err != nil {
		return nil, grpcx.Wrap(err, "save review vote")
	}
	s.log.InfoContext(ctx, "escalate review vote", "tenant", req.GetContext().GetTenantId())
	advanceSellerListingWorkflowRequest := &sellersv1.AdvanceSellerListingWorkflowRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetApprover().GetEmail()},
		},
		Details: &sellersv1.AdvanceSellerListingWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetReviewVote().GetSubject().GetEmail()},
		},
	}
	advanceSellerListingWorkflowReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*sellersv1.AdvanceSellerListingWorkflowResponse, error) {
		return s.sellerListingWorkflowClient.AdvanceSellerListingWorkflow(ctx, advanceSellerListingWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingWorkflowService/AdvanceSellerListingWorkflow")
	}
	return &reviewsv1.EscalateReviewVoteResponse{
		ReviewVote: &reviewsv1.ReviewVote{
			Measurements: []*reviewsv1.ReviewVoteMeasurement{&reviewsv1.ReviewVoteMeasurement{
				Unit: advanceSellerListingWorkflowReply.GetSellerListing().GetDimensions().GetUnit(),
			}},
			ReviewVoteId: req.GetReviewVoteId(),
			Source:       req.GetReviewVote().GetSource(),
		},
		Outcome: &reviewsv1.EscalateReviewVoteResponse_Outcome{
			Status: advanceSellerListingWorkflowReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// DelegateReviewVoteApproval delegates a review vote.
func (s *ReviewVoteApprovalServer) DelegateReviewVoteApproval(ctx context.Context, req *reviewsv1.DelegateReviewVoteApprovalRequest) (*reviewsv1.DelegateReviewVoteApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReviewVote(ctx, req.GetContext().GetTenantId(), req.GetReviewVote()); err != nil {
		return nil, grpcx.Wrap(err, "save review vote")
	}
	s.log.InfoContext(ctx, "delegate review vote approval", "tenant", req.GetContext().GetTenantId())
	attributes := make(map[string]string, len(req.GetReviewVote().GetAttributes()))
	for key, value := range req.GetReviewVote().GetAttributes() {
		attributes[key] = value
	}
	var envelope reviewsv1.ProductReviewRef
	if err := req.GetReviewVote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	summarizeWorkOrdersRequest := &maintenancev1.SummarizeWorkOrdersRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: req.GetAnnotations(),
		Details:     &maintenancev1.SummarizeWorkOrdersRequest_Details{Filters: attributes},
		WorkOrderId: envelope.GetDisplayName(),
	}
	type summarizeWorkOrdersResult struct {
		reply *maintenancev1.SummarizeWorkOrdersResponse
		err   error
	}
	summarizeWorkOrdersResults := make(chan summarizeWorkOrdersResult, 1)
	go func() {
		reply, err := s.workOrderQueryClient.SummarizeWorkOrders(ctx, summarizeWorkOrdersRequest)
		summarizeWorkOrdersResults <- summarizeWorkOrdersResult{reply, err}
	}()
	var summarizeWorkOrdersReply *maintenancev1.SummarizeWorkOrdersResponse
	select {
	case result := <-summarizeWorkOrdersResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.maintenance.v1.WorkOrderQueryService/SummarizeWorkOrders")
		}
		summarizeWorkOrdersReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*reviewsv1.RejectProductReviewResponse, len(req.GetReviewVote().GetMeasurements()))
	for i, measurement := range req.GetReviewVote().GetMeasurements() {
		group.Go(func() error {
			reply, err := s.productReviewApprovalClient.RejectProductReview(groupCtx, &reviewsv1.RejectProductReviewRequest{
				Context:       req.GetContext(),
				ProductReview: &reviewsv1.ProductReview{IssuedAt: measurement.GetObservedAt()},
				Details: &reviewsv1.RejectProductReviewRequest_Details{
					Approver: &identityv1.UserRef{
						DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
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
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewApprovalService/RejectProductReview")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &reviewsv1.DelegateReviewVoteApprovalResponse{
		Result: &commonv1.OperationResult{
			Details: summarizeWorkOrdersReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		ReviewVote: &reviewsv1.ReviewVote{
			ReviewVoteId:  req.GetReviewVoteId(),
			CorrelationId: req.GetReviewVote().GetCorrelationId(),
		},
	}, nil
}

// RecallReviewVote recalls a review vote.
func (s *ReviewVoteApprovalServer) RecallReviewVote(ctx context.Context, req *reviewsv1.RecallReviewVoteRequest) (*reviewsv1.RecallReviewVoteResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecallReviewVote", "error", err)
		return nil, err
	}
	if err := s.store.SaveReviewVote(ctx, req.GetContext().GetTenantId(), req.GetReviewVote()); err != nil {
		return nil, grpcx.Wrap(err, "save review vote")
	}
	s.log.InfoContext(ctx, "recall review vote", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetReviewVote())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope reviewsv1.ProductReviewRef
	if err := req.GetReviewVote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	lintReviewVotesRequest := &reviewsv1.LintReviewVotesRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				Email:       req.GetDetails().GetApprover().GetEmail(),
				DisplayName: req.GetReviewVote().GetSubject().GetDisplayName(),
			},
			Detail: packed,
		},
		ReviewVoteId: envelope.GetDisplayName(),
	}
	lintReviewVotesReply := new(reviewsv1.LintReviewVotesResponse)
	if err := grpcx.Invoke(ctx, s.conn, reviewsv1.ReviewVoteValidationService_LintReviewVotes_FullMethodName, lintReviewVotesRequest, lintReviewVotesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteValidationService/LintReviewVotes")
	}
	attributes := make(map[string]string, len(req.GetReviewVote().GetAttributes()))
	for key, value := range req.GetReviewVote().GetAttributes() {
		attributes[key] = value
	}
	var envelope2 reviewsv1.ProductReviewRef
	if err := req.GetReviewVote().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	startGiftCardBatchWorkflowRequest := &giftcardsv1.StartGiftCardBatchWorkflowRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Lines: []*giftcardsv1.GiftCardBatchLine{&giftcardsv1.GiftCardBatchLine{Attributes: attributes}},
			Counterparty: &giftcardsv1.GiftCardRef{
				DisplayName: req.GetReviewVote().GetSubject().GetDisplayName(),
			},
		},
		Details: &giftcardsv1.StartGiftCardBatchWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetReviewVote().GetSubject().GetEmail()},
		},
		Annotations:     req.GetAnnotations(),
		GiftCardBatchId: envelope2.GetDisplayName(),
	}
	startGiftCardBatchWorkflowReply, err := giftcardsv1.GiftCardBatchWorkflowServiceClient.StartGiftCardBatchWorkflow(s.giftCardBatchWorkflowClient, ctx, startGiftCardBatchWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchWorkflowService/StartGiftCardBatchWorkflow")
	}
	return &reviewsv1.RecallReviewVoteResponse{
		ReviewVote: &reviewsv1.ReviewVote{
			CorrelationId: lintReviewVotesReply.GetReviewVote().GetCorrelationId(),
			ReviewVoteId:  req.GetReviewVoteId(),
		},
		Result: &commonv1.OperationResult{Status: lintReviewVotesReply.GetOutcome().GetStatus()},
		Outcome: &reviewsv1.RecallReviewVoteResponse_Outcome{
			ReferenceId: startGiftCardBatchWorkflowReply.GetOutcome().GetReferenceId(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	}, nil
}

// GetReviewVoteApprovalStatus returns a review vote.
func (s *ReviewVoteApprovalServer) GetReviewVoteApprovalStatus(ctx context.Context, req *reviewsv1.GetReviewVoteApprovalStatusRequest) (*reviewsv1.GetReviewVoteApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetReviewVoteApprovalStatus", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "review_vote", req.GetReviewVoteId(), "get_review_vote_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get review vote approval status", "tenant", req.GetContext().GetTenantId())
	return &reviewsv1.GetReviewVoteApprovalStatusResponse{
		ReviewVote: &reviewsv1.ReviewVote{ReviewVoteId: req.GetReviewVoteId()},
		Result:     &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentReviewVoteApproval comments a review vote.
func (s *ReviewVoteApprovalServer) CommentReviewVoteApproval(ctx context.Context, req *reviewsv1.CommentReviewVoteApprovalRequest) (*reviewsv1.CommentReviewVoteApprovalResponse, error) {
	if err := s.store.SaveReviewVote(ctx, req.GetContext().GetTenantId(), req.GetReviewVote()); err != nil {
		return nil, grpcx.Wrap(err, "save review vote")
	}
	s.log.InfoContext(ctx, "comment review vote approval", "tenant", req.GetContext().GetTenantId())
	reviewModerationApproval, ok := s.clients.Get("acme.reviews.v1.ReviewModerationApprovalService").(reviewsv1.ReviewModerationApprovalServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.reviews.v1.ReviewModerationApprovalService")
	}
	var holder struct{ DisplayName string }
	if record := req.GetDetails().GetApprover(); record != nil {
		if err := mapper.CopyFields(&holder, record, "DisplayName"); err != nil {
			s.log.WarnContext(ctx, "copy display_name", "error", err)
		}
	}
	approveReviewModerationRequest := &reviewsv1.ApproveReviewModerationRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Assignee: &identityv1.UserRef{
				DisplayName: holder.DisplayName,
				UserId:      req.GetDetails().GetApprover().GetUserId(),
			},
		},
	}
	approveReviewModerationReply, err := reviewModerationApproval.ApproveReviewModeration(ctx, approveReviewModerationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationApprovalService/ApproveReviewModeration")
	}
	var envelope reviewsv1.ProductReviewRef
	if err := req.GetReviewVote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	archiveTenantRequest := &identityv1.ArchiveTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Details: &identityv1.ArchiveTenantRequest_Details{
			NotifyContact: &commonv1.ContactInfo{
				Email: approveReviewModerationReply.GetReviewModeration().GetAssignee().GetEmail(),
			},
			NewOwner:   &identityv1.UserRef{Email: req.GetDetails().GetApprover().GetEmail()},
			ReasonCode: envelope.GetDisplayName(),
		},
	}
	archiveTenantReply, err := grpcx.Call(ctx, s.tenantAdminClient.ArchiveTenant, archiveTenantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/ArchiveTenant")
	}
	var envelope2 reviewsv1.ProductReviewRef
	if err := req.GetReviewVote().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	suggestHouseholdsRequest := &customersv1.SuggestHouseholdsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.SuggestHouseholdsRequest_Filter{
			Region:       archiveTenantReply.GetTenant().GetLocation().GetRegion(),
			CreatedAfter: approveReviewModerationReply.GetReviewModeration().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetDetails().GetApprover().GetUserId(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope2.GetDisplayName()},
	}
	suggestHouseholdsReply, err := s.householdQueryLazy().SuggestHouseholds(ctx, suggestHouseholdsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/SuggestHouseholds")
	}
	return &reviewsv1.CommentReviewVoteApprovalResponse{
		Outcome: &reviewsv1.CommentReviewVoteApprovalResponse_Outcome{
			ReferenceId: approveReviewModerationReply.GetOutcome().GetReferenceId(),
			EffectiveAt: archiveTenantReply.GetOutcome().GetEffectiveAt(),
			Message:     suggestHouseholdsReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: archiveTenantReply.GetResult().GetCorrelationId(),
			Message:       suggestHouseholdsReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		ReviewVote: &reviewsv1.ReviewVote{
			ReviewVoteId: req.GetReviewVoteId(),
			Attributes:   req.GetReviewVote().GetAttributes(),
		},
	}, nil
}

// householdQueryLazy dials acme.customers.v1.HouseholdQueryService the first time a handler needs it.
func (s *ReviewVoteApprovalServer) householdQueryLazy() customersv1.HouseholdQueryServiceClient {
	s.householdQueryOnce.Do(func() { s.householdQueryCached = customersv1.NewHouseholdQueryServiceClient(s.conn) })
	return s.householdQueryCached
}
