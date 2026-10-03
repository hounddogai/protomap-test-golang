package reviews

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	forecastingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/forecasting/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ProductReviewQueryServer implements `acme.reviews.v1.ProductReviewQueryService`.
// Answers read-only questions about product reviews, for screens and reports.
type ProductReviewQueryServer struct {
	reviewsv1.UnimplementedProductReviewQueryServiceServer
	store                            *Store
	log                              *slog.Logger
	placementValidationClient        merchandisingv1.PlacementValidationServiceClient
	sellerListingCalculationClient   sellersv1.SellerListingCalculationServiceClient
	productReviewValidationClient    reviewsv1.ProductReviewValidationServiceClient
	sessionQueryClient               identityv1.SessionQueryServiceClient
	sessionQueryClientFallback       identityv1.SessionQueryServiceClient
	reviewVoteCalculationClient      reviewsv1.ReviewVoteCalculationServiceClient
	sellerListingValidationClient    sellersv1.SellerListingValidationServiceClient
	metricSeriesQueryClient          metricsv1.MetricSeriesQueryServiceClient
	metricSeriesQueryClientFallback  metricsv1.MetricSeriesQueryServiceClient
	reviewModerationValidationClient reviewsv1.ReviewModerationValidationServiceClient
	meteredPlacementValidation       meteredPlacementValidation
	demandForecastClient             forecastingv1.DemandForecastServiceClient
}

// NewProductReviewQueryServer wires a ProductReviewQueryServer.
func NewProductReviewQueryServer(deps platform.Deps) *ProductReviewQueryServer {
	return &ProductReviewQueryServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.reviews.v1.ProductReviewQueryService"),
		placementValidationClient:        merchandisingv1.NewPlacementValidationServiceClient(deps.Conn),
		sellerListingCalculationClient:   sellersv1.NewSellerListingCalculationServiceClient(deps.Conn),
		productReviewValidationClient:    reviewsv1.NewProductReviewValidationServiceClient(deps.Conn),
		sessionQueryClient:               identityv1.NewSessionQueryServiceClient(deps.Conn),
		sessionQueryClientFallback:       identityv1.NewSessionQueryServiceClient(deps.Conn),
		reviewVoteCalculationClient:      reviewsv1.NewReviewVoteCalculationServiceClient(deps.Conn),
		sellerListingValidationClient:    sellersv1.NewSellerListingValidationServiceClient(deps.Conn),
		metricSeriesQueryClient:          metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn),
		metricSeriesQueryClientFallback:  metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn),
		reviewModerationValidationClient: reviewsv1.NewReviewModerationValidationServiceClient(deps.Conn),
		meteredPlacementValidation:       meteredPlacementValidation{PlacementValidationServiceClient: merchandisingv1.NewPlacementValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		demandForecastClient:             forecastingv1.NewDemandForecastServiceClient(deps.Conn),
	}
}

// LookupProductReview returns a product review.
func (s *ProductReviewQueryServer) LookupProductReview(ctx context.Context, req *reviewsv1.LookupProductReviewRequest) (*reviewsv1.LookupProductReviewResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LookupProductReview", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product_review", req.GetProductReviewId(), "lookup_product_review"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup product review", "tenant", req.GetContext().GetTenantId())
	return &reviewsv1.LookupProductReviewResponse{
		ProductReview: &reviewsv1.ProductReview{ProductReviewId: req.GetProductReviewId()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BrowseProductReviews lists product reviews a page at a time.
func (s *ProductReviewQueryServer) BrowseProductReviews(ctx context.Context, req *reviewsv1.BrowseProductReviewsRequest) (*reviewsv1.BrowseProductReviewsResponse, error) {
	s.log.InfoContext(ctx, "browse product reviews", "tenant", req.GetContext().GetTenantId())
	return &reviewsv1.BrowseProductReviewsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// CountProductReviews counts product reviews by status.
func (s *ProductReviewQueryServer) CountProductReviews(ctx context.Context, req *reviewsv1.CountProductReviewsRequest) (*reviewsv1.CountProductReviewsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CountProductReviews", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "count product reviews", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	normalizePlacementRequest := &merchandisingv1.NormalizePlacementRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Details: &merchandisingv1.NormalizePlacementRequest_Details{
			Contact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
			Locale:  req.GetContext().GetLocale(),
		},
	}
	normalizePlacementReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*merchandisingv1.NormalizePlacementResponse, error) {
		return s.placementValidationClient.NormalizePlacement(ctx, normalizePlacementRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementValidationService/NormalizePlacement")
	}
	return &reviewsv1.CountProductReviewsResponse{
		Result: &commonv1.OperationResult{Status: normalizePlacementReply.GetResult().GetStatus()},
	}, nil
}

// ResolveProductReviews returns several product reviews at once.
func (s *ProductReviewQueryServer) ResolveProductReviews(ctx context.Context, req *reviewsv1.ResolveProductReviewsRequest) (*reviewsv1.ResolveProductReviewsResponse, error) {
	s.log.InfoContext(ctx, "resolve product reviews", "tenant", req.GetContext().GetTenantId())
	source := req.GetContext().GetActor().ProtoReflect()
	userId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("user_id"))).String()
	quoteSellerListingReply, err := s.sellerListingCalculationClient.QuoteSellerListing(ctx, &sellersv1.QuoteSellerListingRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: userId,
			},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
			Tags:  req.GetProductReviewIds(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingCalculationService/QuoteSellerListing")
	}
	return &reviewsv1.ResolveProductReviewsResponse{
		ProductReviews: []*reviewsv1.ProductReview{&reviewsv1.ProductReview{
			BillingAddress: &commonv1.PostalAddress{
				Locality: quoteSellerListingReply.GetSellerListing().GetAddress().GetLocality(),
			},
			Lines: []*reviewsv1.ProductReviewLine{&reviewsv1.ProductReviewLine{
				Description: quoteSellerListingReply.GetSellerListing().GetComponents()["primary"].GetDescription(),
			}},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExportProductReviews streams every matching product review.
func (s *ProductReviewQueryServer) ExportProductReviews(req *reviewsv1.ExportProductReviewsRequest, stream grpc.ServerStreamingServer[reviewsv1.ExportProductReviewsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		return err
	}
	s.log.InfoContext(ctx, "export product reviews", "tenant", req.GetContext().GetTenantId())
	explainProductReviewDecisionRequest := &reviewsv1.ExplainProductReviewDecisionRequest{
		Context: req.GetContext(),
		ProductReview: &reviewsv1.ProductReview{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			Counterparty:   &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	explainProductReviewDecisionReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*reviewsv1.ExplainProductReviewDecisionResponse, error) {
		return s.productReviewValidationClient.ExplainProductReviewDecision(ctx, explainProductReviewDecisionRequest)
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.reviews.v1.ProductReviewValidationService/ExplainProductReviewDecision")
	}
	routes := map[string]func(context.Context, *identityv1.FindSessionsRequest, ...grpc.CallOption) (*identityv1.FindSessionsResponse, error){
		"primary":  s.sessionQueryClient.FindSessions,
		"fallback": s.sessionQueryClientFallback.FindSessions,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	ctx = tenancy.With(ctx, "region", req.GetFilter().GetRegion())
	findSessionsRequest := &identityv1.FindSessionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.FindSessionsRequest_Filter{
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			Region:       tenancy.Value(ctx, "region"),
			OwnerId:      req.GetFilter().GetOwnerId(),
			Query:        req.GetFilter().GetQuery(),
		},
	}
	findSessionsReply, err := route(ctx, findSessionsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/FindSessions")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := productReviewQueryServerRecalculateReviewVotesSteps[mode]
	recalculateReviewVotesRequest := &reviewsv1.RecalculateReviewVotesRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				Email:      explainProductReviewDecisionReply.GetProductReview().GetCounterparty().GetEmail(),
				CustomerId: explainProductReviewDecisionReply.GetProductReview().GetCounterparty().GetCustomerId(),
			},
		},
		Details: &reviewsv1.RecalculateReviewVotesRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: explainProductReviewDecisionReply.GetProductReview().GetBillingAddress().GetPostalCode(),
			},
		},
	}
	recalculateReviewVotesReply, err := run(s, ctx, recalculateReviewVotesRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteCalculationService/RecalculateReviewVotes")
	}
	step := &productReviewQueryServerExportProductReviewsNormalizeSellerListingStep{request: &sellersv1.NormalizeSellerListingRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{
				DisplayName: explainProductReviewDecisionReply.GetProductReview().GetCounterparty().GetDisplayName(),
			},
		},
		Details: &sellersv1.NormalizeSellerListingRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: explainProductReviewDecisionReply.GetProductReview().GetCounterparty().GetEmail(),
			},
		},
	}}
	if err := s.runExportProductReviewsNormalizeSellerListing(ctx, step); err != nil {
		return grpcx.Wrap(err, "acme.sellers.v1.SellerListingValidationService/NormalizeSellerListing")
	}
	normalizeSellerListingReply := step.reply
	if err := stream.Send(&reviewsv1.ExportProductReviewsResponse{
		ProductReview: &reviewsv1.ProductReview{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: explainProductReviewDecisionReply.GetProductReview().GetAudit().GetUpdatedAt(),
			},
			Lines: []*reviewsv1.ProductReviewLine{&reviewsv1.ProductReviewLine{
				Attributes: recalculateReviewVotesReply.GetReviewVote().GetAttributes(),
			}},
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Result: &commonv1.OperationResult{
			Details:       findSessionsReply.GetResult().GetDetails(),
			Status:        findSessionsReply.GetResult().GetStatus(),
			ReferenceIds:  recalculateReviewVotesReply.GetResult().GetReferenceIds(),
			CorrelationId: normalizeSellerListingReply.GetResult().GetCorrelationId(),
		},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizeProductReviews summarizes a product review.
func (s *ProductReviewQueryServer) SummarizeProductReviews(ctx context.Context, req *reviewsv1.SummarizeProductReviewsRequest) (*reviewsv1.SummarizeProductReviewsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product_review", req.GetProductReviewId(), "summarize_product_reviews"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize product reviews", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *metricsv1.SuggestMetricSeriesesRequest, ...grpc.CallOption) (*metricsv1.SuggestMetricSeriesesResponse, error){
		"primary":  s.metricSeriesQueryClient.SuggestMetricSerieses,
		"fallback": s.metricSeriesQueryClientFallback.SuggestMetricSerieses,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	suggestMetricSeriesesRequest := &metricsv1.SuggestMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter: &metricsv1.SuggestMetricSeriesesRequest_Filter{
			Labels:       req.GetDetails().GetFilters(),
			CreatedAfter: req.GetContext().GetDeadline(),
			OwnerId:      req.GetProductReviewId(),
		},
	}
	suggestMetricSeriesesReply, err := route(ctx, suggestMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/SuggestMetricSerieses")
	}
	return &reviewsv1.SummarizeProductReviewsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: suggestMetricSeriesesReply.GetResult().GetCorrelationId(),
			ReferenceIds:  suggestMetricSeriesesReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestProductReviews lists product reviews a page at a time.
func (s *ProductReviewQueryServer) SuggestProductReviews(ctx context.Context, req *reviewsv1.SuggestProductReviewsRequest) (*reviewsv1.SuggestProductReviewsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "suggest product reviews", "tenant", req.GetContext().GetTenantId())
	validateReviewModerationRequest := &reviewsv1.ValidateReviewModerationRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.ValidateReviewModerationRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		ReviewModeration: &reviewsv1.ReviewModeration{
			Audit:    &commonv1.AuditInfo{UpdatedAt: req.GetFilter().GetCreatedAfter()},
			Assignee: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}
	type validateReviewModerationResult struct {
		reply *reviewsv1.ValidateReviewModerationResponse
		err   error
	}
	validateReviewModerationResults := make(chan validateReviewModerationResult, 1)
	go func() {
		reply, err := s.reviewModerationValidationClient.ValidateReviewModeration(ctx, validateReviewModerationRequest)
		validateReviewModerationResults <- validateReviewModerationResult{reply, err}
	}()
	var validateReviewModerationReply *reviewsv1.ValidateReviewModerationResponse
	select {
	case result := <-validateReviewModerationResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.reviews.v1.ReviewModerationValidationService/ValidateReviewModeration")
		}
		validateReviewModerationReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &reviewsv1.SuggestProductReviewsResponse{
		ProductReviews: []*reviewsv1.ProductReview{&reviewsv1.ProductReview{
			Counterparty: &customersv1.CustomerRef{
				CustomerId: validateReviewModerationReply.GetReviewModeration().GetCustomer().GetCustomerId(),
			},
		}},
		Result: &commonv1.OperationResult{
			Status: validateReviewModerationReply.GetResult().GetStatus(),
		},
	}, nil
}

// PeekProductReview returns a product review.
func (s *ProductReviewQueryServer) PeekProductReview(ctx context.Context, req *reviewsv1.PeekProductReviewRequest) (*reviewsv1.PeekProductReviewResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product_review", req.GetProductReviewId(), "peek_product_review"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek product review", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "locale", req.GetContext().GetLocale())
	validatePlacementRequest := &merchandisingv1.ValidatePlacementRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Organizer: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
		Details: &merchandisingv1.ValidatePlacementRequest_Details{Locale: tenancy.Value(ctx, "locale")},
	}
	validatePlacementReply, err := s.meteredPlacementValidation.ValidatePlacement(ctx, validatePlacementRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementValidationService/ValidatePlacement")
	}
	cloneDemandForecastReply, err := s.demandForecastClient.CloneDemandForecast(ctx, &forecastingv1.CloneDemandForecastRequest{
		Context: req.GetContext(),
		DemandForecast: &forecastingv1.DemandForecast{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: validatePlacementReply.GetPlacement().GetAudit().GetUpdatedAt(),
				Etag:      validatePlacementReply.GetPlacement().GetAudit().GetEtag(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Lines: validatePlacementReply.GetPlacement().GetLocation().GetLines(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.forecasting.v1.DemandForecastService/CloneDemandForecast")
	}
	return &reviewsv1.PeekProductReviewResponse{
		ProductReview: &reviewsv1.ProductReview{
			Audit: &commonv1.AuditInfo{
				Etag: validatePlacementReply.GetPlacement().GetAudit().GetEtag(),
			},
			ProductReviewId: req.GetProductReviewId(),
		},
		Result: &commonv1.OperationResult{
			Message: cloneDemandForecastReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// productReviewQueryServerRecalculateReviewVotesSteps are the ways to run
// acme.reviews.v1.ReviewVoteCalculationService/RecalculateReviewVotes, by name.
var productReviewQueryServerRecalculateReviewVotesSteps = map[string]func(*ProductReviewQueryServer, context.Context, *reviewsv1.RecalculateReviewVotesRequest) (*reviewsv1.RecalculateReviewVotesResponse, error){
	"default": (*ProductReviewQueryServer).recalculateReviewVotesStep,
	"audited": (*ProductReviewQueryServer).recalculateReviewVotesAuditedStep,
}

func (s *ProductReviewQueryServer) recalculateReviewVotesStep(ctx context.Context, request *reviewsv1.RecalculateReviewVotesRequest) (*reviewsv1.RecalculateReviewVotesResponse, error) {
	return s.reviewVoteCalculationClient.RecalculateReviewVotes(ctx, request)
}

func (s *ProductReviewQueryServer) recalculateReviewVotesAuditedStep(ctx context.Context, request *reviewsv1.RecalculateReviewVotesRequest) (*reviewsv1.RecalculateReviewVotesResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.reviews.v1.ReviewVoteCalculationService/RecalculateReviewVotes")
	return s.reviewVoteCalculationClient.RecalculateReviewVotes(ctx, request)
}

// productReviewQueryServerExportProductReviewsNormalizeSellerListingStep carries the request of one step of
// ExportProductReviews and, once it ran, its reply.
type productReviewQueryServerExportProductReviewsNormalizeSellerListingStep struct {
	request *sellersv1.NormalizeSellerListingRequest
	reply   *sellersv1.NormalizeSellerListingResponse
}

func (s *ProductReviewQueryServer) runExportProductReviewsNormalizeSellerListing(ctx context.Context, step *productReviewQueryServerExportProductReviewsNormalizeSellerListingStep) error {
	reply, err := s.sellerListingValidationClient.NormalizeSellerListing(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
