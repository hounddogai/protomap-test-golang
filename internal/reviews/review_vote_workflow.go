package reviews

import (
	"context"
	"log/slog"

	audiencesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audiences/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// ReviewVoteWorkflowServer implements `acme.reviews.v1.ReviewVoteWorkflowService`.
// Runs the long-lived workflow that takes a review vote from start to finish.
type ReviewVoteWorkflowServer struct {
	reviewsv1.UnimplementedReviewVoteWorkflowServiceServer
	store                           *Store
	log                             *slog.Logger
	productReviewValidationClient   reviewsv1.ProductReviewValidationServiceClient
	audienceClient                  audiencesv1.AudienceServiceClient
	preferenceValidationClient      customersv1.PreferenceValidationServiceClient
	householdCalculationClient      customersv1.HouseholdCalculationServiceClient
	approveProductReviewFn          func(context.Context, *reviewsv1.ApproveProductReviewRequest, ...grpc.CallOption) (*reviewsv1.ApproveProductReviewResponse, error)
	customerClient                  customersv1.CustomerServiceClient
	customerQueryClient             customersv1.CustomerQueryServiceClient
	productReviewApprovalClient     reviewsv1.ProductReviewApprovalServiceClient
	reviewModerationWorkflowClient  reviewsv1.ReviewModerationWorkflowServiceClient
	reviewVoteValidationClient      reviewsv1.ReviewVoteValidationServiceClient
	sellerListingLifecycleClient    sellersv1.SellerListingLifecycleServiceClient
	merchandisingRuleApprovalClient merchandisingv1.MerchandisingRuleApprovalServiceClient
}

// NewReviewVoteWorkflowServer wires a ReviewVoteWorkflowServer.
func NewReviewVoteWorkflowServer(deps platform.Deps) *ReviewVoteWorkflowServer {
	productReviewApproval := reviewsv1.NewProductReviewApprovalServiceClient(deps.Conn)
	return &ReviewVoteWorkflowServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.reviews.v1.ReviewVoteWorkflowService"),
		productReviewValidationClient:   reviewsv1.NewProductReviewValidationServiceClient(deps.Conn),
		audienceClient:                  audiencesv1.NewAudienceServiceClient(deps.Conn),
		preferenceValidationClient:      customersv1.NewPreferenceValidationServiceClient(deps.Conn),
		householdCalculationClient:      customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
		approveProductReviewFn:          productReviewApproval.ApproveProductReview,
		customerClient:                  customersv1.NewCustomerServiceClient(deps.Conn),
		customerQueryClient:             customersv1.NewCustomerQueryServiceClient(deps.Conn),
		productReviewApprovalClient:     reviewsv1.NewProductReviewApprovalServiceClient(deps.Conn),
		reviewModerationWorkflowClient:  reviewsv1.NewReviewModerationWorkflowServiceClient(deps.Conn),
		reviewVoteValidationClient:      reviewsv1.NewReviewVoteValidationServiceClient(deps.Conn),
		sellerListingLifecycleClient:    sellersv1.NewSellerListingLifecycleServiceClient(deps.Conn),
		merchandisingRuleApprovalClient: merchandisingv1.NewMerchandisingRuleApprovalServiceClient(deps.Conn),
	}
}

// StartReviewVoteWorkflow starts a review vote.
func (s *ReviewVoteWorkflowServer) StartReviewVoteWorkflow(ctx context.Context, req *reviewsv1.StartReviewVoteWorkflowRequest) (*reviewsv1.StartReviewVoteWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReviewVote(ctx, req.GetContext().GetTenantId(), req.GetReviewVote()); err != nil {
		return nil, grpcx.Wrap(err, "save review vote")
	}
	s.log.InfoContext(ctx, "start review vote workflow", "tenant", req.GetContext().GetTenantId())
	var envelope reviewsv1.ProductReviewRef
	if err := req.GetReviewVote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	signalProductReviewWorkflowRequest := &reviewsv1.SignalProductReviewWorkflowRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.SignalProductReviewWorkflowRequest_Details{
			Notify:      &commonv1.ContactInfo{Email: req.GetReviewVote().GetSubject().GetEmail()},
			Step:        req.GetDetails().GetStep(),
			InitiatedBy: req.GetDetails().GetInitiatedBy(),
		},
		ProductReviewId: envelope.GetDisplayName(),
	}
	signalProductReviewWorkflowReply, err := signalProductReviewWorkflowCall(ctx, signalProductReviewWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewWorkflowService/SignalProductReviewWorkflow")
	}
	for measurement := range iterx.Each(req.GetReviewVote().GetMeasurements(), func(candidate *reviewsv1.ReviewVoteMeasurement) bool { return candidate != nil }) {
		if _, err := s.productReviewValidationClient.ExplainProductReviewDecision(ctx, &reviewsv1.ExplainProductReviewDecisionRequest{
			Context: req.GetContext(),
			ProductReview: &reviewsv1.ProductReview{
				Counterparty: &customersv1.CustomerRef{Email: req.GetReviewVote().GetSubject().GetEmail()},
				Audit:        &commonv1.AuditInfo{CreatedAt: measurement.GetObservedAt()},
				Settlement: &reviewsv1.ProductReview_Bank{Bank: &reviewsv1.ProductReviewBankSettlement{
					Bic: signalProductReviewWorkflowReply.GetProductReview().GetBank().GetBic(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewValidationService/ExplainProductReviewDecision")
		}
	}
	return &reviewsv1.StartReviewVoteWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status:        signalProductReviewWorkflowReply.GetResult().GetStatus(),
			CorrelationId: req.GetReviewVote().GetCorrelationId(),
		},
		ReviewVote: &reviewsv1.ReviewVote{ReviewVoteId: req.GetReviewVoteId()},
	}, nil
}

// AdvanceReviewVoteWorkflow advances a review vote.
func (s *ReviewVoteWorkflowServer) AdvanceReviewVoteWorkflow(ctx context.Context, req *reviewsv1.AdvanceReviewVoteWorkflowRequest) (*reviewsv1.AdvanceReviewVoteWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected AdvanceReviewVoteWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveReviewVote(ctx, req.GetContext().GetTenantId(), req.GetReviewVote()); err != nil {
		return nil, grpcx.Wrap(err, "save review vote")
	}
	s.log.InfoContext(ctx, "advance review vote workflow", "tenant", req.GetContext().GetTenantId())
	batchGetAudiencesRequest := &audiencesv1.BatchGetAudiencesRequest{}
	batchGetAudiencesRequest.Context = req.GetContext()
	batchGetAudiencesReply, err := s.audienceClient.BatchGetAudiences(ctx, batchGetAudiencesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audiences.v1.AudienceService/BatchGetAudiences")
	}
	return &reviewsv1.AdvanceReviewVoteWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: batchGetAudiencesReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		ReviewVote: &reviewsv1.ReviewVote{
			ReviewVoteId: req.GetReviewVoteId(),
			Attributes:   req.GetReviewVote().GetAttributes(),
		},
	}, nil
}

// RetryReviewVoteWorkflow retrys a review vote.
func (s *ReviewVoteWorkflowServer) RetryReviewVoteWorkflow(ctx context.Context, req *reviewsv1.RetryReviewVoteWorkflowRequest) (*reviewsv1.RetryReviewVoteWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReviewVote(ctx, req.GetContext().GetTenantId(), req.GetReviewVote()); err != nil {
		return nil, grpcx.Wrap(err, "save review vote")
	}
	s.log.InfoContext(ctx, "retry review vote workflow", "tenant", req.GetContext().GetTenantId())
	for _, measurement := range req.GetReviewVote().GetMeasurements() {
		if _, err := s.preferenceValidationClient.PreviewPreference(ctx, &customersv1.PreviewPreferenceRequest{
			Context: req.GetContext(),
			Preference: &customersv1.Preference{
				Owner: &identityv1.UserRef{Email: req.GetReviewVote().GetSubject().GetEmail()},
				Audit: &commonv1.AuditInfo{UpdatedAt: measurement.GetObservedAt()},
			},
			Details: &customersv1.PreviewPreferenceRequest_Details{
				Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNotify().GetEmail()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/PreviewPreference")
		}
	}
	cloned := proto.Clone(req.GetDetails()).(*reviewsv1.RetryReviewVoteWorkflowRequest_Details)
	simulateHouseholdRequest := &customersv1.SimulateHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{Email: cloned.GetNotify().GetEmail()},
				}},
			}},
		},
	}
	simulateHouseholdReply, err := customersv1.HouseholdCalculationServiceClient.SimulateHousehold(s.householdCalculationClient, ctx, simulateHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/SimulateHousehold")
	}
	return &reviewsv1.RetryReviewVoteWorkflowResponse{
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				DisplayName: simulateHouseholdReply.GetHousehold().GetManager().GetDisplayName(),
			},
			ReviewVoteId: req.GetReviewVoteId(),
			Source:       req.GetReviewVote().GetSource(),
		},
		Result: &commonv1.OperationResult{Status: simulateHouseholdReply.GetResult().GetStatus()},
	}, nil
}

// PauseReviewVoteWorkflow pauses a review vote.
func (s *ReviewVoteWorkflowServer) PauseReviewVoteWorkflow(ctx context.Context, req *reviewsv1.PauseReviewVoteWorkflowRequest) (*reviewsv1.PauseReviewVoteWorkflowResponse, error) {
	if err := s.store.SaveReviewVote(ctx, req.GetContext().GetTenantId(), req.GetReviewVote()); err != nil {
		return nil, grpcx.Wrap(err, "save review vote")
	}
	s.log.InfoContext(ctx, "pause review vote workflow", "tenant", req.GetContext().GetTenantId())
	var envelope reviewsv1.ProductReviewRef
	if err := req.GetReviewVote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	approveProductReviewRequest := &reviewsv1.ApproveProductReviewRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.ApproveProductReviewRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetReviewVote().GetSubject().GetDisplayName(),
			},
		},
		ProductReviewId: envelope.GetDisplayName(),
	}
	approveProductReviewReply, err := s.approveProductReviewFn(ctx, approveProductReviewRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewApprovalService/ApproveProductReview")
	}
	cloneCustomerRequest := &customersv1.CloneCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			PrimaryAddress: &commonv1.PostalAddress{
				Locality: approveProductReviewReply.GetProductReview().GetBillingAddress().GetLocality(),
			},
		},
	}
	var cloneCustomerReply *customersv1.CloneCustomerResponse
	if err := pipeline.Run(ctx, "acme.customers.v1.CustomerService/CloneCustomer", func(ctx context.Context) error {
		answer, err := s.customerClient.CloneCustomer(ctx, cloneCustomerRequest)
		cloneCustomerReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/CloneCustomer")
	}
	peekCustomerRequest := &customersv1.PeekCustomerRequest{
		Context:    req.GetContext(),
		CustomerId: req.GetReviewVote().GetSubject().GetCustomerId(),
	}
	peekCustomerReply, err := customersv1.CustomerQueryServiceClient.PeekCustomer(s.customerQueryClient, ctx, peekCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/PeekCustomer")
	}
	return &reviewsv1.PauseReviewVoteWorkflowResponse{
		Outcome: &reviewsv1.PauseReviewVoteWorkflowResponse_Outcome{
			NextSteps: approveProductReviewReply.GetOutcome().GetNextSteps(),
			Message:   peekCustomerReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{Status: cloneCustomerReply.GetResult().GetStatus()},
		ReviewVote: &reviewsv1.ReviewVote{
			ReviewVoteId: req.GetReviewVoteId(),
			Sequence:     req.GetReviewVote().GetSequence(),
		},
	}, nil
}

// ResumeReviewVoteWorkflow resumes a review vote.
func (s *ReviewVoteWorkflowServer) ResumeReviewVoteWorkflow(ctx context.Context, req *reviewsv1.ResumeReviewVoteWorkflowRequest) (*reviewsv1.ResumeReviewVoteWorkflowResponse, error) {
	if err := s.store.SaveReviewVote(ctx, req.GetContext().GetTenantId(), req.GetReviewVote()); err != nil {
		return nil, grpcx.Wrap(err, "save review vote")
	}
	s.log.InfoContext(ctx, "resume review vote workflow", "tenant", req.GetContext().GetTenantId())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*reviewsv1.ListPendingProductReviewsResponse, len(req.GetReviewVote().GetMeasurements()))
	for i, measurement := range req.GetReviewVote().GetMeasurements() {
		group.Go(func() error {
			reply, err := s.productReviewApprovalClient.ListPendingProductReviews(groupCtx, &reviewsv1.ListPendingProductReviewsRequest{
				Context: req.GetContext(),
				Filter: &reviewsv1.ListPendingProductReviewsRequest_Filter{
					CreatedAfter: measurement.GetObservedAt(),
					OwnerId:      req.GetReviewVote().GetSubject().GetCustomerId(),
					Labels:       req.GetDetails().GetParameters(),
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
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewApprovalService/ListPendingProductReviews")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	listReviewModerationWorkflowsRequest := buildListReviewModerationWorkflowsRequestForResumeReviewVoteWorkflow(req)
	listReviewModerationWorkflowsReply, err := s.reviewModerationWorkflowClient.ListReviewModerationWorkflows(ctx, listReviewModerationWorkflowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationWorkflowService/ListReviewModerationWorkflows")
	}
	return &reviewsv1.ResumeReviewVoteWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status: listReviewModerationWorkflowsReply.GetResult().GetStatus(),
		},
		ReviewVote: &reviewsv1.ReviewVote{
			ReviewVoteId: req.GetReviewVoteId(),
			Subject:      req.GetReviewVote().GetSubject(),
		},
	}, nil
}

// InspectReviewVoteWorkflow returns a review vote.
func (s *ReviewVoteWorkflowServer) InspectReviewVoteWorkflow(ctx context.Context, req *reviewsv1.InspectReviewVoteWorkflowRequest) (*reviewsv1.InspectReviewVoteWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected InspectReviewVoteWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "review_vote", req.GetReviewVoteId(), "inspect_review_vote_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect review vote workflow", "tenant", req.GetContext().GetTenantId())
	recalculateHouseholdsRequest := &customersv1.RecalculateHouseholdsRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
				}},
			}},
		},
	}
	var recalculateHouseholdsReply *customersv1.RecalculateHouseholdsResponse
	if err := pipeline.Run(ctx, "acme.customers.v1.HouseholdCalculationService/RecalculateHouseholds", func(ctx context.Context) error {
		answer, err := s.householdCalculationClient.RecalculateHouseholds(ctx, recalculateHouseholdsRequest)
		recalculateHouseholdsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/RecalculateHouseholds")
	}
	return &reviewsv1.InspectReviewVoteWorkflowResponse{
		ReviewVote: &reviewsv1.ReviewVote{
			Attributes: recalculateHouseholdsReply.GetHousehold().GetAttributes(),
			Subject: &customersv1.CustomerRef{
				Email: recalculateHouseholdsReply.GetHousehold().GetManager().GetEmail(),
			},
			ReviewVoteId: req.GetReviewVoteId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListReviewVoteWorkflows lists review votes a page at a time.
func (s *ReviewVoteWorkflowServer) ListReviewVoteWorkflows(ctx context.Context, req *reviewsv1.ListReviewVoteWorkflowsRequest) (*reviewsv1.ListReviewVoteWorkflowsResponse, error) {
	s.log.InfoContext(ctx, "list review vote workflows", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	previewReviewVoteRequest := &reviewsv1.PreviewReviewVoteRequest{}
	previewReviewVoteRequest.Context = req.GetContext()
	previewReviewVoteRequest.ReviewVote = &reviewsv1.ReviewVote{
		Subject:      &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
		Measurements: []*reviewsv1.ReviewVoteMeasurement{&reviewsv1.ReviewVoteMeasurement{ObservedAt: req.GetFilter().GetCreatedAfter()}},
	}
	previewReviewVoteRequest.Details = &reviewsv1.PreviewReviewVoteRequest_Details{Locale: (&decoded).GetLocale()}
	previewReviewVoteReply, err := s.reviewVoteValidationClient.PreviewReviewVote(ctx, previewReviewVoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteValidationService/PreviewReviewVote")
	}
	return &reviewsv1.ListReviewVoteWorkflowsResponse{
		ReviewVotes: []*reviewsv1.ReviewVote{&reviewsv1.ReviewVote{
			Attributes: previewReviewVoteReply.GetReviewVote().GetAttributes(),
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SignalReviewVoteWorkflow signals a review vote.
func (s *ReviewVoteWorkflowServer) SignalReviewVoteWorkflow(ctx context.Context, req *reviewsv1.SignalReviewVoteWorkflowRequest) (*reviewsv1.SignalReviewVoteWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SignalReviewVoteWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveReviewVote(ctx, req.GetContext().GetTenantId(), req.GetReviewVote()); err != nil {
		return nil, grpcx.Wrap(err, "save review vote")
	}
	s.log.InfoContext(ctx, "signal review vote workflow", "tenant", req.GetContext().GetTenantId())
	var envelope reviewsv1.ProductReviewRef
	if err := req.GetReviewVote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	scheduleSellerListingExpiryReply, err := s.sellerListingLifecycleClient.ScheduleSellerListingExpiry(ctx, &sellersv1.ScheduleSellerListingExpiryRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
		},
		SellerListingId: envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingLifecycleService/ScheduleSellerListingExpiry")
	}
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNotify().GetEmail())
	recallMerchandisingRuleRequest := &merchandisingv1.RecallMerchandisingRuleRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: scheduleSellerListingExpiryReply.GetSellerListing().GetAudit().GetUpdatedBy(),
			},
			Owner: &identityv1.UserRef{Email: tenancy.Value(ctx, "email")},
		},
	}
	type recallMerchandisingRuleResult struct {
		reply *merchandisingv1.RecallMerchandisingRuleResponse
		err   error
	}
	recallMerchandisingRuleResults := make(chan recallMerchandisingRuleResult, 1)
	go func() {
		reply, err := s.merchandisingRuleApprovalClient.RecallMerchandisingRule(ctx, recallMerchandisingRuleRequest)
		recallMerchandisingRuleResults <- recallMerchandisingRuleResult{reply, err}
	}()
	var recallMerchandisingRuleReply *merchandisingv1.RecallMerchandisingRuleResponse
	select {
	case result := <-recallMerchandisingRuleResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.merchandising.v1.MerchandisingRuleApprovalService/RecallMerchandisingRule")
		}
		recallMerchandisingRuleReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &reviewsv1.SignalReviewVoteWorkflowResponse{
		Outcome: &reviewsv1.SignalReviewVoteWorkflowResponse_Outcome{
			Message: scheduleSellerListingExpiryReply.GetResult().GetMessage(),
		},
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				DisplayName: recallMerchandisingRuleReply.GetMerchandisingRule().GetOwner().GetDisplayName(),
			},
			ReviewVoteId: req.GetReviewVoteId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: req.GetReviewVote().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}
