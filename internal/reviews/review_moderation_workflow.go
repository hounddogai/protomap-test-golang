package reviews

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	showsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shows/v1"
	yardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/yards/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// ReviewModerationWorkflowServer implements `acme.reviews.v1.ReviewModerationWorkflowService`.
// Runs the long-lived workflow that takes a review moderation from start to finish.
type ReviewModerationWorkflowServer struct {
	reviewsv1.UnimplementedReviewModerationWorkflowServiceServer
	store                           *Store
	log                             *slog.Logger
	sellerListingReportingClient    sellersv1.SellerListingReportingServiceClient
	accessPolicyClient              authorizationv1.AccessPolicyServiceClient
	featureFlagClient               featureflagsv1.FeatureFlagServiceClient
	customerCalculationClient       customersv1.CustomerCalculationServiceClient
	sellerScorecardSyncClient       sellersv1.SellerScorecardSyncServiceClient
	delegateSellerListingApprovalFn func(context.Context, *sellersv1.DelegateSellerListingApprovalRequest, ...grpc.CallOption) (*sellersv1.DelegateSellerListingApprovalResponse, error)
	previewReviewModerationFn       func(context.Context, *reviewsv1.PreviewReviewModerationRequest, ...grpc.CallOption) (*reviewsv1.PreviewReviewModerationResponse, error)
	placementWorkflowClient         merchandisingv1.PlacementWorkflowServiceClient
	customerQueryClient             customersv1.CustomerQueryServiceClient
	clients                         *registry.Registry
	reviewModerationApprovalClient  reviewsv1.ReviewModerationApprovalServiceClient
	reviewVoteApprovalClient        reviewsv1.ReviewVoteApprovalServiceClient
	showClient                      showsv1.ShowServiceClient
	preferenceValidationClient      customersv1.PreferenceValidationServiceClient
	yardQueryClient                 yardsv1.YardQueryServiceClient
	reviewVoteValidationClient      reviewsv1.ReviewVoteValidationServiceClient
	conn                            *grpc.ClientConn
	meteredHouseholdValidation      meteredHouseholdValidation
}

// NewReviewModerationWorkflowServer wires a ReviewModerationWorkflowServer.
func NewReviewModerationWorkflowServer(deps platform.Deps) *ReviewModerationWorkflowServer {
	sellerListingApproval := sellersv1.NewSellerListingApprovalServiceClient(deps.Conn)
	reviewModerationValidation := reviewsv1.NewReviewModerationValidationServiceClient(deps.Conn)
	return &ReviewModerationWorkflowServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.reviews.v1.ReviewModerationWorkflowService"),
		sellerListingReportingClient:    sellersv1.NewSellerListingReportingServiceClient(deps.Conn),
		accessPolicyClient:              authorizationv1.NewAccessPolicyServiceClient(deps.Conn),
		featureFlagClient:               featureflagsv1.NewFeatureFlagServiceClient(deps.Conn),
		customerCalculationClient:       customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		sellerScorecardSyncClient:       sellersv1.NewSellerScorecardSyncServiceClient(deps.Conn),
		delegateSellerListingApprovalFn: sellerListingApproval.DelegateSellerListingApproval,
		previewReviewModerationFn:       reviewModerationValidation.PreviewReviewModeration,
		placementWorkflowClient:         merchandisingv1.NewPlacementWorkflowServiceClient(deps.Conn),
		customerQueryClient:             customersv1.NewCustomerQueryServiceClient(deps.Conn),
		clients:                         deps.Clients,
		reviewModerationApprovalClient:  reviewsv1.NewReviewModerationApprovalServiceClient(deps.Conn),
		reviewVoteApprovalClient:        reviewsv1.NewReviewVoteApprovalServiceClient(deps.Conn),
		showClient:                      showsv1.NewShowServiceClient(deps.Conn),
		preferenceValidationClient:      customersv1.NewPreferenceValidationServiceClient(deps.Conn),
		yardQueryClient:                 yardsv1.NewYardQueryServiceClient(deps.Conn),
		reviewVoteValidationClient:      reviewsv1.NewReviewVoteValidationServiceClient(deps.Conn),
		conn:                            deps.Conn,
		meteredHouseholdValidation:      meteredHouseholdValidation{HouseholdValidationServiceClient: customersv1.NewHouseholdValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}
}

// StartReviewModerationWorkflow starts a review moderation.
func (s *ReviewModerationWorkflowServer) StartReviewModerationWorkflow(ctx context.Context, req *reviewsv1.StartReviewModerationWorkflowRequest) (*reviewsv1.StartReviewModerationWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReviewModeration(ctx, req.GetContext().GetTenantId(), req.GetReviewModeration()); err != nil {
		return nil, grpcx.Wrap(err, "save review moderation")
	}
	s.log.InfoContext(ctx, "start review moderation workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReviewModeration().GetAudit().GetEtag())
	streamSellerListingMetricsStream, err := s.sellerListingReportingClient.StreamSellerListingMetrics(ctx, &sellersv1.StreamSellerListingMetricsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Name:  req.GetDetails().GetNotify().GetName(),
			Email: req.GetReviewModeration().GetRequester().GetEmail(),
			Phone: req.GetDetails().GetNotify().GetPhone(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/StreamSellerListingMetrics")
	}
	for {
		received, err := streamSellerListingMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/StreamSellerListingMetrics")
		}
		_ = received
	}
	cloneAccessPolicyRequest := &authorizationv1.CloneAccessPolicyRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetReviewModeration().GetAudit().GetCreatedAt(),
				UpdatedBy: req.GetReviewModeration().GetAudit().GetUpdatedBy(),
			},
		},
	}
	send := s.cloneAccessPolicySender(cloneAccessPolicyRequest)
	cloneAccessPolicyReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/CloneAccessPolicy")
	}
	createFeatureFlagRequest := &featureflagsv1.CreateFeatureFlagRequest{
		Context: req.GetContext(),
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Owner: &identityv1.UserRef{
				Email: cloneAccessPolicyReply.GetAccessPolicy().GetOwner().GetEmail(),
			},
			Action: &featureflagsv1.FeatureFlag_Discount{Discount: &featureflagsv1.FeatureFlagDiscountAction{
				Amount: cloneAccessPolicyReply.GetAccessPolicy().GetDiscount().GetAmount(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetReviewModeration().GetAudit().GetCreatedBy(),
				UpdatedBy: cloneAccessPolicyReply.GetAccessPolicy().GetAudit().GetUpdatedBy(),
			},
		},
	}
	createFeatureFlagReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*featureflagsv1.CreateFeatureFlagResponse, error) {
		return s.featureFlagClient.CreateFeatureFlag(ctx, createFeatureFlagRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagService/CreateFeatureFlag")
	}
	return &reviewsv1.StartReviewModerationWorkflowResponse{
		ReviewModeration: &reviewsv1.ReviewModeration{
			Audit: &commonv1.AuditInfo{
				CreatedBy: cloneAccessPolicyReply.GetAccessPolicy().GetAudit().GetCreatedBy(),
			},
			Requester: &commonv1.ContactInfo{
				Email: createFeatureFlagReply.GetFeatureFlag().GetOwner().GetEmail(),
			},
			ReviewModerationId: req.GetReviewModerationId(),
			Subject:            req.GetReviewModeration().GetSubject(),
		},
		Outcome: &reviewsv1.StartReviewModerationWorkflowResponse_Outcome{Message: createFeatureFlagReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// AdvanceReviewModerationWorkflow advances a review moderation.
func (s *ReviewModerationWorkflowServer) AdvanceReviewModerationWorkflow(ctx context.Context, req *reviewsv1.AdvanceReviewModerationWorkflowRequest) (*reviewsv1.AdvanceReviewModerationWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReviewModeration(ctx, req.GetContext().GetTenantId(), req.GetReviewModeration()); err != nil {
		return nil, grpcx.Wrap(err, "save review moderation")
	}
	s.log.InfoContext(ctx, "advance review moderation workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReviewModeration().GetAudit().GetEtag())
	quoteCustomerRequest := &customersv1.QuoteCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{Email: req.GetReviewModeration().GetAssignee().GetEmail()},
			Contact:             &commonv1.ContactInfo{Name: req.GetReviewModeration().GetRequester().GetName()},
		},
	}
	quoteCustomerReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*customersv1.QuoteCustomerResponse, error) {
		return s.customerCalculationClient.QuoteCustomer(ctx, quoteCustomerRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/QuoteCustomer")
	}
	return &reviewsv1.AdvanceReviewModerationWorkflowResponse{
		ReviewModeration: &reviewsv1.ReviewModeration{
			Requester: &commonv1.ContactInfo{
				PreferredChannel: quoteCustomerReply.GetCustomer().GetContact().GetPreferredChannel(),
			},
			Audit:              &commonv1.AuditInfo{Etag: quoteCustomerReply.GetCustomer().GetAudit().GetEtag()},
			ReviewModerationId: req.GetReviewModerationId(),
			Assignee:           req.GetReviewModeration().GetAssignee(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CancelReviewModerationWorkflow cancels a review moderation.
func (s *ReviewModerationWorkflowServer) CancelReviewModerationWorkflow(ctx context.Context, req *reviewsv1.CancelReviewModerationWorkflowRequest) (*reviewsv1.CancelReviewModerationWorkflowResponse, error) {
	if err := s.store.SaveReviewModeration(ctx, req.GetContext().GetTenantId(), req.GetReviewModeration()); err != nil {
		return nil, grpcx.Wrap(err, "save review moderation")
	}
	s.log.InfoContext(ctx, "cancel review moderation workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReviewModeration().GetAudit().GetEtag())
	parameters := make(map[string]string, len(req.GetDetails().GetParameters()))
	for key, value := range req.GetDetails().GetParameters() {
		parameters[key] = value
	}
	listSellerScorecardConflictsRequest := &sellersv1.ListSellerScorecardConflictsRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.ListSellerScorecardConflictsRequest_Filter{
			CreatedAfter: req.GetReviewModeration().GetAudit().GetUpdatedAt(),
			Labels:       parameters,
			OwnerId:      req.GetReviewModeration().GetReviewModerationId(),
		},
	}
	listSellerScorecardConflictsReply, err := grpcx.Call(ctx, s.sellerScorecardSyncClient.ListSellerScorecardConflicts, listSellerScorecardConflictsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardSyncService/ListSellerScorecardConflicts")
	}
	delegateSellerListingApprovalRequest := &sellersv1.DelegateSellerListingApprovalRequest{
		Context: req.GetContext(),
		Details: &sellersv1.DelegateSellerListingApprovalRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetReviewModeration().GetCustomer().GetEmail()},
		},
	}
	delegateSellerListingApprovalReply, err := s.delegateSellerListingApprovalFn(ctx, delegateSellerListingApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingApprovalService/DelegateSellerListingApproval")
	}
	previewReviewModerationRequest := &reviewsv1.PreviewReviewModerationRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.PreviewReviewModerationRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetReviewModeration().GetRequester().GetEmail()},
		},
		ReviewModeration: &reviewsv1.ReviewModeration{
			Requester: &commonv1.ContactInfo{Phone: req.GetReviewModeration().GetRequester().GetPhone()},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: delegateSellerListingApprovalReply.GetSellerListing().GetAudit().GetUpdatedBy(),
			},
			Origin: &reviewsv1.ReviewModeration_Customer{Customer: &customersv1.CustomerRef{
				Email: delegateSellerListingApprovalReply.GetSellerListing().GetOwner().GetEmail(),
			}},
		},
	}
	previewReviewModerationReply, err := s.previewReviewModerationFn(ctx, previewReviewModerationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationValidationService/PreviewReviewModeration")
	}
	return &reviewsv1.CancelReviewModerationWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details:       listSellerScorecardConflictsReply.GetResult().GetDetails(),
			CorrelationId: delegateSellerListingApprovalReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &reviewsv1.CancelReviewModerationWorkflowResponse_Outcome{
			Status: listSellerScorecardConflictsReply.GetResult().GetStatus(),
		},
		ReviewModeration: &reviewsv1.ReviewModeration{
			Tags: delegateSellerListingApprovalReply.GetSellerListing().GetTags(),
			Sla: &reviewsv1.ReviewModerationSla{
				DueAt: previewReviewModerationReply.GetReviewModeration().GetSla().GetDueAt(),
			},
			ReviewModerationId: req.GetReviewModerationId(),
			Subject:            req.GetReviewModeration().GetSubject(),
		},
	}, nil
}

// RetryReviewModerationWorkflow retrys a review moderation.
func (s *ReviewModerationWorkflowServer) RetryReviewModerationWorkflow(ctx context.Context, req *reviewsv1.RetryReviewModerationWorkflowRequest) (*reviewsv1.RetryReviewModerationWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReviewModeration(ctx, req.GetContext().GetTenantId(), req.GetReviewModeration()); err != nil {
		return nil, grpcx.Wrap(err, "save review moderation")
	}
	s.log.InfoContext(ctx, "retry review moderation workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReviewModeration().GetAudit().GetEtag())
	pausePlacementWorkflowRequest := buildPausePlacementWorkflowRequestForRetryReviewModerationWorkflow(req)
	pausePlacementWorkflowReply, err := s.placementWorkflowClient.PausePlacementWorkflow(ctx, pausePlacementWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementWorkflowService/PausePlacementWorkflow")
	}
	resolveCustomersRequest := &customersv1.ResolveCustomersRequest{Context: req.GetContext()}
	resolveCustomersReply, err := grpcx.Call(ctx, s.customerQueryClient.ResolveCustomers, resolveCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/ResolveCustomers")
	}
	return &reviewsv1.RetryReviewModerationWorkflowResponse{
		ReviewModeration: &reviewsv1.ReviewModeration{
			Comments: []*reviewsv1.ReviewModerationComment{&reviewsv1.ReviewModerationComment{
				CreatedAt: pausePlacementWorkflowReply.GetPlacement().GetAudit().GetCreatedAt(),
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: pausePlacementWorkflowReply.GetPlacement().GetAudit().GetUpdatedAt(),
			},
			ReviewModerationId: req.GetReviewModerationId(),
			Priority:           req.GetReviewModeration().GetPriority(),
		},
		Result: &commonv1.OperationResult{
			Details: resolveCustomersReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// PauseReviewModerationWorkflow pauses a review moderation.
func (s *ReviewModerationWorkflowServer) PauseReviewModerationWorkflow(ctx context.Context, req *reviewsv1.PauseReviewModerationWorkflowRequest) (*reviewsv1.PauseReviewModerationWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReviewModeration(ctx, req.GetContext().GetTenantId(), req.GetReviewModeration()); err != nil {
		return nil, grpcx.Wrap(err, "save review moderation")
	}
	s.log.InfoContext(ctx, "pause review moderation workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReviewModeration().GetAudit().GetEtag())
	tenantExport, ok := s.clients.Get("acme.identity.v1.TenantExportService").(identityv1.TenantExportServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.identity.v1.TenantExportService")
	}
	getTenantExportRequest := &identityv1.GetTenantExportRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
	}
	getTenantExportReply, err := tenantExport.GetTenantExport(ctx, getTenantExportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/GetTenantExport")
	}
	reviewModerationCache := cache.New[string, *reviewsv1.ReviewModeration](1)
	reviewModerationCache.Put(req.GetContext().GetTraceId(), req.GetReviewModeration())
	cachedReviewModeration, _ := reviewModerationCache.Get(req.GetContext().GetTraceId())
	commentReviewModerationApprovalRequest := &reviewsv1.CommentReviewModerationApprovalRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Assignee:  &identityv1.UserRef{Email: req.GetReviewModeration().GetAssignee().GetEmail()},
			Requester: &commonv1.ContactInfo{Name: cachedReviewModeration.GetRequester().GetName()},
		},
		Details: &reviewsv1.CommentReviewModerationApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetReviewModeration().GetAssignee().GetDisplayName(),
			},
		},
	}
	commentReviewModerationApprovalReply, err := reviewsv1.ReviewModerationApprovalServiceClient.CommentReviewModerationApproval(s.reviewModerationApprovalClient, ctx, commentReviewModerationApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationApprovalService/CommentReviewModerationApproval")
	}
	packed, err := anypb.New(req.GetReviewModeration())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	escalateReviewVoteRequest := &reviewsv1.EscalateReviewVoteRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{Email: req.GetReviewModeration().GetAssignee().GetEmail()},
			Detail:  packed,
		},
	}
	type escalateReviewVoteResult struct {
		reply *reviewsv1.EscalateReviewVoteResponse
		err   error
	}
	escalateReviewVoteResults := make(chan escalateReviewVoteResult, 1)
	go func() {
		reply, err := s.reviewVoteApprovalClient.EscalateReviewVote(ctx, escalateReviewVoteRequest)
		escalateReviewVoteResults <- escalateReviewVoteResult{reply, err}
	}()
	var escalateReviewVoteReply *reviewsv1.EscalateReviewVoteResponse
	select {
	case result := <-escalateReviewVoteResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.reviews.v1.ReviewVoteApprovalService/EscalateReviewVote")
		}
		escalateReviewVoteReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &reviewsv1.PauseReviewModerationWorkflowResponse{
		ReviewModeration: &reviewsv1.ReviewModeration{
			Audit: &commonv1.AuditInfo{
				CreatedBy: getTenantExportReply.GetTenant().GetAudit().GetCreatedBy(),
				UpdatedBy: commentReviewModerationApprovalReply.GetReviewModeration().GetAudit().GetUpdatedBy(),
			},
			Assignee: &identityv1.UserRef{
				UserId: getTenantExportReply.GetTenant().GetManager().GetUserId(),
			},
			ReviewModerationId: req.GetReviewModerationId(),
			Subject:            req.GetReviewModeration().GetSubject(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: commentReviewModerationApprovalReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &reviewsv1.PauseReviewModerationWorkflowResponse_Outcome{
			EffectiveAt: escalateReviewVoteReply.GetOutcome().GetEffectiveAt(),
		},
	}, nil
}

// ResumeReviewModerationWorkflow resumes a review moderation.
func (s *ReviewModerationWorkflowServer) ResumeReviewModerationWorkflow(ctx context.Context, req *reviewsv1.ResumeReviewModerationWorkflowRequest) (*reviewsv1.ResumeReviewModerationWorkflowResponse, error) {
	if err := s.store.SaveReviewModeration(ctx, req.GetContext().GetTenantId(), req.GetReviewModeration()); err != nil {
		return nil, grpcx.Wrap(err, "save review moderation")
	}
	s.log.InfoContext(ctx, "resume review moderation workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReviewModeration().GetAudit().GetEtag())
	jobs := make(chan *reviewsv1.ReviewModerationComment)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for comment := range jobs {
				if _, err := s.showClient.ListShows(workCtx, &showsv1.ListShowsRequest{
					Context: req.GetContext(),
					Filter:  &showsv1.ListShowsRequest_Filter{CreatedAfter: comment.GetCreatedAt()},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetReviewModeration().GetComments() {
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
		return nil, grpcx.Wrap(err, "acme.shows.v1.ShowService/ListShows")
	}
	return &reviewsv1.ResumeReviewModerationWorkflowResponse{
		ReviewModeration: &reviewsv1.ReviewModeration{
			ReviewModerationId: req.GetReviewModerationId(),
			Assignee:           req.GetReviewModeration().GetAssignee(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// InspectReviewModerationWorkflow returns a review moderation.
func (s *ReviewModerationWorkflowServer) InspectReviewModerationWorkflow(ctx context.Context, req *reviewsv1.InspectReviewModerationWorkflowRequest) (*reviewsv1.InspectReviewModerationWorkflowResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "review_moderation", req.GetReviewModerationId(), "inspect_review_moderation_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect review moderation workflow", "tenant", req.GetContext().GetTenantId())
	normalizePreferenceRequest := &customersv1.NormalizePreferenceRequest{
		Context: req.GetContext(),
		Details: &customersv1.NormalizePreferenceRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
		},
	}
	normalizePreferenceReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*customersv1.NormalizePreferenceResponse, error) {
		return s.preferenceValidationClient.NormalizePreference(ctx, normalizePreferenceRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/NormalizePreference")
	}
	return &reviewsv1.InspectReviewModerationWorkflowResponse{
		ReviewModeration: &reviewsv1.ReviewModeration{
			Comments: []*reviewsv1.ReviewModerationComment{&reviewsv1.ReviewModerationComment{
				CreatedAt: normalizePreferenceReply.GetPreference().GetAudit().GetCreatedAt(),
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: normalizePreferenceReply.GetPreference().GetAudit().GetUpdatedBy(),
			},
			ReviewModerationId: req.GetReviewModerationId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListReviewModerationWorkflows lists review moderations a page at a time.
func (s *ReviewModerationWorkflowServer) ListReviewModerationWorkflows(ctx context.Context, req *reviewsv1.ListReviewModerationWorkflowsRequest) (*reviewsv1.ListReviewModerationWorkflowsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list review moderation workflows", "tenant", req.GetContext().GetTenantId())
	var holder struct{ OwnerId string }
	if record := req.GetFilter(); record != nil {
		if err := mapper.CopyFields(&holder, record, "OwnerId"); err != nil {
			s.log.WarnContext(ctx, "copy owner_id", "error", err)
		}
	}
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	exportYardsStream, err := s.yardQueryClient.ExportYards(ctx, &yardsv1.ExportYardsRequest{
		Context: req.GetContext(),
		Filter: &yardsv1.ExportYardsRequest_Filter{
			Query:        req.GetFilter().GetQuery(),
			OwnerId:      holder.OwnerId,
			Labels:       labels,
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.yards.v1.YardQueryService/ExportYards")
	}
	for {
		received, err := exportYardsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.yards.v1.YardQueryService/ExportYards")
		}
		_ = received
	}
	validateReviewVoteRequest := newValidateReviewVoteRequest(withValidateReviewVoteContext(req.GetContext()), withValidateReviewVoteReviewVote(&reviewsv1.ReviewVote{
		Subject:    &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
		OccurredAt: req.GetFilter().GetCreatedAfter(),
	}))
	validateReviewVoteReply, err := s.reviewVoteValidationClient.ValidateReviewVote(ctx, validateReviewVoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteValidationService/ValidateReviewVote")
	}
	return &reviewsv1.ListReviewModerationWorkflowsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: validateReviewVoteReply.GetReviewVote().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// SignalReviewModerationWorkflow signals a review moderation.
func (s *ReviewModerationWorkflowServer) SignalReviewModerationWorkflow(ctx context.Context, req *reviewsv1.SignalReviewModerationWorkflowRequest) (*reviewsv1.SignalReviewModerationWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReviewModeration(ctx, req.GetContext().GetTenantId(), req.GetReviewModeration()); err != nil {
		return nil, grpcx.Wrap(err, "save review moderation")
	}
	s.log.InfoContext(ctx, "signal review moderation workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReviewModeration().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "etag", req.GetReviewModeration().GetAudit().GetEtag())
	recallSellerRequest := &sellersv1.RecallSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{Email: req.GetReviewModeration().GetAssignee().GetEmail()},
			Contact:             &commonv1.ContactInfo{Phone: req.GetReviewModeration().GetRequester().GetPhone()},
			Audit:               &commonv1.AuditInfo{Etag: tenancy.Value(ctx, "etag")},
		},
	}
	recallSellerReply := new(sellersv1.RecallSellerResponse)
	if err := grpc.Invoke(ctx, "/acme.sellers.v1.SellerApprovalService/RecallSeller", recallSellerRequest, recallSellerReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerApprovalService/RecallSeller")
	}
	explainHouseholdDecisionRequest := &customersv1.ExplainHouseholdDecisionRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit: &commonv1.AuditInfo{
						CreatedAt: recallSellerReply.GetSeller().GetAudit().GetCreatedAt(),
					},
					Manager: &identityv1.UserRef{
						Email:  recallSellerReply.GetSeller().GetContact().GetEmail(),
						UserId: recallSellerReply.GetSeller().GetRelationshipManager().GetUserId(),
					},
				}},
			}},
		},
	}
	explainHouseholdDecisionReply, err := s.meteredHouseholdValidation.ExplainHouseholdDecision(ctx, explainHouseholdDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/ExplainHouseholdDecision")
	}
	recallPlacementRequest := &merchandisingv1.RecallPlacementRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Participants: []*merchandisingv1.PlacementParticipant{&merchandisingv1.PlacementParticipant{
				Contact: &commonv1.ContactInfo{
					PreferredChannel: req.GetReviewModeration().GetRequester().GetPreferredChannel(),
					Phone:            req.GetReviewModeration().GetRequester().GetPhone(),
				},
			}},
		},
	}
	recallPlacementReply := new(merchandisingv1.RecallPlacementResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", merchandisingPlacementApprovalServiceName, "RecallPlacement"), recallPlacementRequest, recallPlacementReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementApprovalService/RecallPlacement")
	}
	return &reviewsv1.SignalReviewModerationWorkflowResponse{
		ReviewModeration: &reviewsv1.ReviewModeration{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: recallSellerReply.GetSeller().GetAudit().GetUpdatedBy(),
			},
			ReviewModerationId: req.GetReviewModerationId(),
			Requester:          req.GetReviewModeration().GetRequester(),
		},
		Outcome: &reviewsv1.SignalReviewModerationWorkflowResponse_Outcome{
			EffectiveAt: recallSellerReply.GetOutcome().GetEffectiveAt(),
			ReferenceId: recallPlacementReply.GetOutcome().GetReferenceId(),
		},
		Result: &commonv1.OperationResult{
			Message: explainHouseholdDecisionReply.GetOutcome().GetMessage(),
			Details: explainHouseholdDecisionReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// cloneAccessPolicySender returns a function that sends one request to
// acme.authorization.v1.AccessPolicyService/CloneAccessPolicy.
func (s *ReviewModerationWorkflowServer) cloneAccessPolicySender(request *authorizationv1.CloneAccessPolicyRequest) func(context.Context) (*authorizationv1.CloneAccessPolicyResponse, error) {
	return func(ctx context.Context) (*authorizationv1.CloneAccessPolicyResponse, error) {
		return s.accessPolicyClient.CloneAccessPolicy(ctx, request)
	}
}
