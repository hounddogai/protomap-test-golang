package reviews

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	employeesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/employees/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// ReviewModerationQueryServer implements `acme.reviews.v1.ReviewModerationQueryService`.
// Answers read-only questions about review moderations, for screens and reports.
type ReviewModerationQueryServer struct {
	reviewsv1.UnimplementedReviewModerationQueryServiceServer
	store                                 *Store
	log                                   *slog.Logger
	bus                                   *eventbus.Bus
	reviewModerationCalculationClient     reviewsv1.ReviewModerationCalculationServiceClient
	employeeQueryClient                   employeesv1.EmployeeQueryServiceClient
	productReviewValidationClient         reviewsv1.ProductReviewValidationServiceClient
	clients                               *registry.Registry
	sellerPayoutScheduleCalculationClient sellersv1.SellerPayoutScheduleCalculationServiceClient
	venueClient                           venuesv1.VenueServiceClient
	reviewVoteCalculationClient           reviewsv1.ReviewVoteCalculationServiceClient
	conn                                  *grpc.ClientConn
	productReviewQueryClient              reviewsv1.ProductReviewQueryServiceClient
	productReviewQueryClientFallback      reviewsv1.ProductReviewQueryServiceClient
}

// NewReviewModerationQueryServer wires a ReviewModerationQueryServer.
func NewReviewModerationQueryServer(deps platform.Deps) *ReviewModerationQueryServer {
	return &ReviewModerationQueryServer{
		store:                                 NewStore(deps.DB),
		log:                                   deps.Logger.With("service", "acme.reviews.v1.ReviewModerationQueryService"),
		bus:                                   deps.Bus,
		reviewModerationCalculationClient:     reviewsv1.NewReviewModerationCalculationServiceClient(deps.Conn),
		employeeQueryClient:                   employeesv1.NewEmployeeQueryServiceClient(deps.Conn),
		productReviewValidationClient:         reviewsv1.NewProductReviewValidationServiceClient(deps.Conn),
		clients:                               deps.Clients,
		sellerPayoutScheduleCalculationClient: sellersv1.NewSellerPayoutScheduleCalculationServiceClient(deps.Conn),
		venueClient:                           venuesv1.NewVenueServiceClient(deps.Conn),
		reviewVoteCalculationClient:           reviewsv1.NewReviewVoteCalculationServiceClient(deps.Conn),
		conn:                                  deps.Conn,
		productReviewQueryClient:              reviewsv1.NewProductReviewQueryServiceClient(deps.Conn),
		productReviewQueryClientFallback:      reviewsv1.NewProductReviewQueryServiceClient(deps.Conn),
	}
}

// LookupReviewModeration returns a review moderation.
func (s *ReviewModerationQueryServer) LookupReviewModeration(ctx context.Context, req *reviewsv1.LookupReviewModerationRequest) (*reviewsv1.LookupReviewModerationResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "review_moderation", req.GetReviewModerationId(), "lookup_review_moderation"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup review moderation", "tenant", req.GetContext().GetTenantId())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "reviews.lookup_review_moderation.suggest_sponsors",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"filter.owner_id":   req.GetReviewModerationId(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	getReviewModerationRatesRequest := buildGetReviewModerationRatesRequestForLookupReviewModeration(req)
	getReviewModerationRatesReply, err := s.reviewModerationCalculationClient.GetReviewModerationRates(ctx, getReviewModerationRatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationCalculationService/GetReviewModerationRates")
	}
	return &reviewsv1.LookupReviewModerationResponse{
		ReviewModeration: &reviewsv1.ReviewModeration{
			Requester: &commonv1.ContactInfo{
				Email: getReviewModerationRatesReply.GetReviewModeration().GetRequester().GetEmail(),
			},
			ReviewModerationId: req.GetReviewModerationId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BrowseReviewModerations lists review moderations a page at a time.
func (s *ReviewModerationQueryServer) BrowseReviewModerations(ctx context.Context, req *reviewsv1.BrowseReviewModerationsRequest) (*reviewsv1.BrowseReviewModerationsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "browse review moderations", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "page_token", req.GetPage().GetPageToken())
	step := &reviewModerationQueryServerBrowseReviewModerationsBrowseEmployeesStep{request: &employeesv1.BrowseEmployeesRequest{
		Context: req.GetContext(),
		Page:    &commonv1.PageRequest{PageToken: tenancy.Value(ctx, "page_token")},
		Filter:  &employeesv1.BrowseEmployeesRequest_Filter{Region: req.GetFilter().GetRegion()},
	}}
	if err := s.runBrowseReviewModerationsBrowseEmployees(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.employees.v1.EmployeeQueryService/BrowseEmployees")
	}
	browseEmployeesReply := step.reply
	return &reviewsv1.BrowseReviewModerationsResponse{
		PageInfo: &commonv1.PageInfo{
			TotalSize: browseEmployeesReply.GetPageInfo().GetTotalSize(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// FindReviewModerations lists review moderations a page at a time.
func (s *ReviewModerationQueryServer) FindReviewModerations(ctx context.Context, req *reviewsv1.FindReviewModerationsRequest) (*reviewsv1.FindReviewModerationsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "find review moderations", "tenant", req.GetContext().GetTenantId())
	explainProductReviewDecisionRequest := &reviewsv1.ExplainProductReviewDecisionRequest{
		Context: req.GetContext(),
		ProductReview: &reviewsv1.ProductReview{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			Counterparty:   &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	var explainProductReviewDecisionReply *reviewsv1.ExplainProductReviewDecisionResponse
	if err := pipeline.Run(ctx, "acme.reviews.v1.ProductReviewValidationService/ExplainProductReviewDecision", func(ctx context.Context) error {
		answer, err := s.productReviewValidationClient.ExplainProductReviewDecision(ctx, explainProductReviewDecisionRequest)
		explainProductReviewDecisionReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewValidationService/ExplainProductReviewDecision")
	}
	return &reviewsv1.FindReviewModerationsResponse{
		ReviewModerations: []*reviewsv1.ReviewModeration{&reviewsv1.ReviewModeration{
			Audit: &commonv1.AuditInfo{
				CreatedAt: explainProductReviewDecisionReply.GetProductReview().GetAudit().GetCreatedAt(),
			},
			Assignee: &identityv1.UserRef{
				Email: explainProductReviewDecisionReply.GetProductReview().GetCounterparty().GetEmail(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ResolveReviewModerations returns several review moderations at once.
func (s *ReviewModerationQueryServer) ResolveReviewModerations(ctx context.Context, req *reviewsv1.ResolveReviewModerationsRequest) (*reviewsv1.ResolveReviewModerationsResponse, error) {
	s.log.InfoContext(ctx, "resolve review moderations", "tenant", req.GetContext().GetTenantId())
	reviewVoteCalculation, err := registry.Lookup[reviewsv1.ReviewVoteCalculationServiceClient](s.clients, "acme.reviews.v1.ReviewVoteCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	recalculateReviewVotesRequest := &reviewsv1.RecalculateReviewVotesRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject:    &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
			OccurredAt: req.GetContext().GetDeadline(),
			Detail:     packed,
		},
	}
	recalculateReviewVotesReply, err := reviewVoteCalculation.RecalculateReviewVotes(ctx, recalculateReviewVotesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteCalculationService/RecalculateReviewVotes")
	}
	calculateSellerPayoutScheduleRequest := &sellersv1.CalculateSellerPayoutScheduleRequest{}
	calculateSellerPayoutScheduleRequest.Context = req.GetContext()
	calculateSellerPayoutScheduleRequest.SellerPayoutSchedule = &sellersv1.SellerPayoutSchedule{
		Organizer: &identityv1.UserRef{
			Email: recalculateReviewVotesReply.GetReviewVote().GetSubject().GetEmail(),
		},
	}
	calculateSellerPayoutScheduleReply, err := s.sellerPayoutScheduleCalculationClient.CalculateSellerPayoutSchedule(ctx, calculateSellerPayoutScheduleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/CalculateSellerPayoutSchedule")
	}
	return &reviewsv1.ResolveReviewModerationsResponse{
		Result: &commonv1.OperationResult{
			Status:  recalculateReviewVotesReply.GetResult().GetStatus(),
			Message: calculateSellerPayoutScheduleReply.GetResult().GetMessage(),
		},
		ReviewModerations: []*reviewsv1.ReviewModeration{&reviewsv1.ReviewModeration{
			Assignee: &identityv1.UserRef{
				DisplayName: recalculateReviewVotesReply.GetReviewVote().GetSubject().GetDisplayName(),
			},
		}},
	}, nil
}

// ExportReviewModerations streams every matching review moderation.
func (s *ReviewModerationQueryServer) ExportReviewModerations(req *reviewsv1.ExportReviewModerationsRequest, stream grpc.ServerStreamingServer[reviewsv1.ExportReviewModerationsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExportReviewModerations", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "export review moderations", "tenant", req.GetContext().GetTenantId())
	cloneVenueRequest := &venuesv1.CloneVenueRequest{
		Context: req.GetContext(),
		Venue: &venuesv1.Venue{
			Locator: &venuesv1.Venue_Address{Address: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()}},
			Owner:   &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}
	type cloneVenueResult struct {
		reply *venuesv1.CloneVenueResponse
		err   error
	}
	cloneVenueResults := make(chan cloneVenueResult, 1)
	go func() {
		reply, err := s.venueClient.CloneVenue(ctx, cloneVenueRequest)
		cloneVenueResults <- cloneVenueResult{reply, err}
	}()
	var cloneVenueReply *venuesv1.CloneVenueResponse
	select {
	case result := <-cloneVenueResults:
		if result.err != nil {
			return grpcx.Wrap(result.err, "acme.venues.v1.VenueService/CloneVenue")
		}
		cloneVenueReply = result.reply
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
	if err := stream.Send(&reviewsv1.ExportReviewModerationsResponse{
		ReviewModeration: &reviewsv1.ReviewModeration{
			Comments: []*reviewsv1.ReviewModerationComment{&reviewsv1.ReviewModerationComment{
				CreatedAt: cloneVenueReply.GetVenue().GetAudit().GetCreatedAt(),
			}},
		},
		Result: &commonv1.OperationResult{
			CorrelationId: cloneVenueReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizeReviewModerations summarizes a review moderation.
func (s *ReviewModerationQueryServer) SummarizeReviewModerations(ctx context.Context, req *reviewsv1.SummarizeReviewModerationsRequest) (*reviewsv1.SummarizeReviewModerationsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SummarizeReviewModerations", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "review_moderation", req.GetReviewModerationId(), "summarize_review_moderations"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize review moderations", "tenant", req.GetContext().GetTenantId())
	estimateReviewVoteRequest := &reviewsv1.EstimateReviewVoteRequest{}
	estimateReviewVoteRequest.Context = req.GetContext()
	estimateReviewVoteRequest.ReviewVote = &reviewsv1.ReviewVote{
		Subject: &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
	}
	estimateReviewVoteReply, err := s.reviewVoteCalculationClient.EstimateReviewVote(ctx, estimateReviewVoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteCalculationService/EstimateReviewVote")
	}
	return &reviewsv1.SummarizeReviewModerationsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: estimateReviewVoteReply.GetReviewVote().GetCorrelationId(),
			Status:        "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestReviewModerations lists review moderations a page at a time.
func (s *ReviewModerationQueryServer) SuggestReviewModerations(ctx context.Context, req *reviewsv1.SuggestReviewModerationsRequest) (*reviewsv1.SuggestReviewModerationsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "suggest review moderations", "tenant", req.GetContext().GetTenantId())
	resolveProductReviewsRequest := &reviewsv1.ResolveProductReviewsRequest{Context: req.GetContext()}
	resolveProductReviewsReply, err := grpcx.NewClient(s.conn, reviewsv1.NewProductReviewQueryServiceClient).ResolveProductReviews(ctx, resolveProductReviewsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewQueryService/ResolveProductReviews")
	}
	return &reviewsv1.SuggestReviewModerationsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: resolveProductReviewsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// PeekReviewModeration returns a review moderation.
func (s *ReviewModerationQueryServer) PeekReviewModeration(ctx context.Context, req *reviewsv1.PeekReviewModerationRequest) (*reviewsv1.PeekReviewModerationResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "review_moderation", req.GetReviewModerationId(), "peek_review_moderation"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek review moderation", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *reviewsv1.BrowseProductReviewsRequest, ...grpc.CallOption) (*reviewsv1.BrowseProductReviewsResponse, error){
		"primary":  s.productReviewQueryClient.BrowseProductReviews,
		"fallback": s.productReviewQueryClientFallback.BrowseProductReviews,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	ctx = tenancy.With(ctx, "review_moderation_id", req.GetReviewModerationId())
	browseProductReviewsRequest := &reviewsv1.BrowseProductReviewsRequest{
		Context: req.GetContext(),
		Filter: &reviewsv1.BrowseProductReviewsRequest_Filter{
			OwnerId:      tenancy.Value(ctx, "review_moderation_id"),
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	}
	browseProductReviewsReply, err := route(ctx, browseProductReviewsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewQueryService/BrowseProductReviews")
	}
	return &reviewsv1.PeekReviewModerationResponse{
		Result: &commonv1.OperationResult{
			Message:      browseProductReviewsReply.GetResult().GetMessage(),
			ReferenceIds: browseProductReviewsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		ReviewModeration: &reviewsv1.ReviewModeration{ReviewModerationId: req.GetReviewModerationId()},
	}, nil
}

// reviewModerationQueryServerBrowseReviewModerationsBrowseEmployeesStep carries the request of one step of
// BrowseReviewModerations and, once it ran, its reply.
type reviewModerationQueryServerBrowseReviewModerationsBrowseEmployeesStep struct {
	request *employeesv1.BrowseEmployeesRequest
	reply   *employeesv1.BrowseEmployeesResponse
}

func (s *ReviewModerationQueryServer) runBrowseReviewModerationsBrowseEmployees(ctx context.Context, step *reviewModerationQueryServerBrowseReviewModerationsBrowseEmployeesStep) error {
	reply, err := s.employeeQueryClient.BrowseEmployees(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
