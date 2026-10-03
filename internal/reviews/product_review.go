package reviews

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	onboardingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/onboarding/v1"
	payrollv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payroll/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// ProductReviewServer implements `acme.reviews.v1.ProductReviewService`.
// Manages product reviews: creates, reads, changes, and lists them.
type ProductReviewServer struct {
	reviewsv1.UnsafeProductReviewServiceServer
	store                             *Store
	log                               *slog.Logger
	reviewModerationCalculationClient reviewsv1.ReviewModerationCalculationServiceClient
	reviewModerationCalculationPort   reviewModerationCalculationPort
	reviewVoteCalculationClient       reviewsv1.ReviewVoteCalculationServiceClient
	synonymValidationClient           searchv1.SynonymValidationServiceClient
	searchQueryNotificationClient     searchv1.SearchQueryNotificationServiceClient
	listSearchIndexExportsFn          func(context.Context, *searchv1.ListSearchIndexExportsRequest, ...grpc.CallOption) (*searchv1.ListSearchIndexExportsResponse, error)
	reviewVoteValidationClient        reviewsv1.ReviewVoteValidationServiceClient
	onboardingPlanCalculationClient   onboardingv1.OnboardingPlanCalculationServiceClient
	payRunClient                      payrollv1.PayRunServiceClient
	reviewModerationValidationClient  reviewsv1.ReviewModerationValidationServiceClient
}

// NewProductReviewServer wires a ProductReviewServer.
func NewProductReviewServer(deps platform.Deps) *ProductReviewServer {
	searchIndexExport := searchv1.NewSearchIndexExportServiceClient(deps.Conn)
	return &ProductReviewServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.reviews.v1.ProductReviewService"),
		reviewModerationCalculationClient: reviewsv1.NewReviewModerationCalculationServiceClient(deps.Conn),
		reviewModerationCalculationPort:   newReviewModerationCalculationPort(deps),
		reviewVoteCalculationClient:       reviewsv1.NewReviewVoteCalculationServiceClient(deps.Conn),
		synonymValidationClient:           searchv1.NewSynonymValidationServiceClient(deps.Conn),
		searchQueryNotificationClient:     searchv1.NewSearchQueryNotificationServiceClient(deps.Conn),
		listSearchIndexExportsFn:          searchIndexExport.ListSearchIndexExports,
		reviewVoteValidationClient:        reviewsv1.NewReviewVoteValidationServiceClient(deps.Conn),
		onboardingPlanCalculationClient:   onboardingv1.NewOnboardingPlanCalculationServiceClient(deps.Conn),
		payRunClient:                      payrollv1.NewPayRunServiceClient(deps.Conn),
		reviewModerationValidationClient:  reviewsv1.NewReviewModerationValidationServiceClient(deps.Conn),
	}
}

// CreateProductReview creates a product review.
func (s *ProductReviewServer) CreateProductReview(ctx context.Context, req *reviewsv1.CreateProductReviewRequest) (*reviewsv1.CreateProductReviewResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CreateProductReview", "error", err)
		return nil, err
	}
	if err := s.store.SaveProductReview(ctx, req.GetContext().GetTenantId(), req.GetProductReview()); err != nil {
		return nil, grpcx.Wrap(err, "save product review")
	}
	s.log.InfoContext(ctx, "create product review", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductReview().GetAudit().GetEtag())
	recalculateReviewModerationsRequest := &reviewsv1.RecalculateReviewModerationsRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Origin: &reviewsv1.ReviewModeration_Customer{Customer: &customersv1.CustomerRef{Email: req.GetProductReview().GetCounterparty().GetEmail()}},
			Audit:  &commonv1.AuditInfo{UpdatedAt: req.GetProductReview().GetAudit().GetUpdatedAt()},
			Assignee: &identityv1.UserRef{
				DisplayName: req.GetProductReview().GetCounterparty().GetDisplayName(),
			},
		},
		Details: &reviewsv1.RecalculateReviewModerationsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: req.GetProductReview().GetBillingAddress().GetRecipient(),
			},
		},
	}
	recalculateReviewModerationsReply, err := grpcx.Call(ctx, s.reviewModerationCalculationClient.RecalculateReviewModerations, recalculateReviewModerationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationCalculationService/RecalculateReviewModerations")
	}
	updateSellerRequest := &sellersv1.UpdateSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			PrimaryAddress: &commonv1.PostalAddress{
				Locality: req.GetProductReview().GetBillingAddress().GetLocality(),
			},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetProductReview().GetAudit().GetCreatedAt()},
		},
	}
	updateSellerReply, err := updateSellerCall(ctx, updateSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerService/UpdateSeller")
	}
	recentProductReviews.Store(recordKey{req.GetContext().GetTenantId(), req.GetProductReview().GetProductReviewId()}, req.GetProductReview())
	return &reviewsv1.CreateProductReviewResponse{
		ProductReview: &reviewsv1.ProductReview{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: recalculateReviewModerationsReply.GetReviewModeration().GetAudit().GetUpdatedAt(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Region: updateSellerReply.GetSeller().GetPrimaryAddress().GetRegion(),
			},
			Number: updateSellerReply.GetSeller().GetNationalId().GetNumber(),
			Total:  req.GetProductReview().GetTotal(),
			Status: req.GetProductReview().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetProductReview returns a product review.
func (s *ProductReviewServer) GetProductReview(ctx context.Context, req *reviewsv1.GetProductReviewRequest) (*reviewsv1.GetProductReviewResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product_review", req.GetProductReviewId(), "get_product_review"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get product review", "tenant", req.GetContext().GetTenantId())
	return &reviewsv1.GetProductReviewResponse{
		ProductReview: &reviewsv1.ProductReview{ProductReviewId: req.GetProductReviewId()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// DeleteProductReview deletes a product review.
func (s *ProductReviewServer) DeleteProductReview(ctx context.Context, req *reviewsv1.DeleteProductReviewRequest) (*reviewsv1.DeleteProductReviewResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product_review", req.GetProductReviewId(), "delete_product_review"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete product review", "tenant", req.GetContext().GetTenantId())
	var cachedNumber string
	if entry, ok := recentProductReviews.Load(recordKey{req.GetContext().GetTenantId(), req.GetProductReviewId()}); ok {
		if previous, ok := entry.(*reviewsv1.ProductReview); ok {
			cachedNumber = previous.GetNumber()
		}
	}
	estimateReviewModerationRequest := &reviewsv1.EstimateReviewModerationRequest{
		Context:          req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{Audit: &commonv1.AuditInfo{Etag: req.GetEtag()}},
		Reason:           cachedNumber,
	}
	estimateReviewModerationReply, err := s.reviewModerationCalculationPort.EstimateReviewModeration(ctx, estimateReviewModerationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationCalculationService/EstimateReviewModeration")
	}
	calculateReviewVoteRequest := &reviewsv1.CalculateReviewVoteRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				Email:       estimateReviewModerationReply.GetReviewModeration().GetRequester().GetEmail(),
				CustomerId:  estimateReviewModerationReply.GetReviewModeration().GetCustomer().GetCustomerId(),
				DisplayName: estimateReviewModerationReply.GetReviewModeration().GetAssignee().GetDisplayName(),
			},
		},
	}
	calculateReviewVoteReply, err := grpcx.Call(ctx, s.reviewVoteCalculationClient.CalculateReviewVote, calculateReviewVoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteCalculationService/CalculateReviewVote")
	}
	checkSynonymEligibilityReply, err := s.synonymValidationClient.CheckSynonymEligibility(ctx, &searchv1.CheckSynonymEligibilityRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{
				Email:       estimateReviewModerationReply.GetReviewModeration().GetRequester().GetEmail(),
				DisplayName: estimateReviewModerationReply.GetReviewModeration().GetCustomer().GetDisplayName(),
			},
			Action: &searchv1.Synonym_Block{Block: &searchv1.SynonymBlockAction{
				NotifyEmail: calculateReviewVoteReply.GetReviewVote().GetSubject().GetEmail(),
			}},
		},
		Details: &searchv1.CheckSynonymEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone: estimateReviewModerationReply.GetReviewModeration().GetRequester().GetPhone(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/CheckSynonymEligibility")
	}
	subscribeSearchQueryUpdatesReply, err := s.searchQueryNotificationClient.SubscribeSearchQueryUpdates(ctx, &searchv1.SubscribeSearchQueryUpdatesRequest{
		Context: req.GetContext(),
		Details: &searchv1.SubscribeSearchQueryUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Name:  estimateReviewModerationReply.GetReviewModeration().GetRequester().GetName(),
				Email: checkSynonymEligibilityReply.GetSynonym().GetOwner().GetEmail(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/SubscribeSearchQueryUpdates")
	}
	return &reviewsv1.DeleteProductReviewResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  estimateReviewModerationReply.GetResult().GetReferenceIds(),
			Details:       calculateReviewVoteReply.GetResult().GetDetails(),
			Message:       checkSynonymEligibilityReply.GetResult().GetMessage(),
			CorrelationId: checkSynonymEligibilityReply.GetResult().GetCorrelationId(),
			Status:        subscribeSearchQueryUpdatesReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// ListProductReviews lists product reviews a page at a time.
func (s *ProductReviewServer) ListProductReviews(ctx context.Context, req *reviewsv1.ListProductReviewsRequest) (*reviewsv1.ListProductReviewsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list product reviews", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetFilter()).(*reviewsv1.ListProductReviewsRequest_Filter)
	cloned2 := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	step := &productReviewServerListProductReviewsRecalculateReviewModerationsStep{request: &reviewsv1.RecalculateReviewModerationsRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.RecalculateReviewModerationsRequest_Details{
			Destination: &commonv1.PostalAddress{Region: cloned.GetRegion()},
		},
		ReviewModeration: &reviewsv1.ReviewModeration{
			Assignee: &identityv1.UserRef{Email: cloned2.GetActor().GetEmail()},
		},
	}}
	if err := s.runListProductReviewsRecalculateReviewModerations(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationCalculationService/RecalculateReviewModerations")
	}
	recalculateReviewModerationsReply := step.reply
	listSearchIndexExportsRequest := &searchv1.ListSearchIndexExportsRequest{
		Context: req.GetContext(),
		Filter:  &searchv1.ListSearchIndexExportsRequest_Filter{CreatedAfter: req.GetFilter().GetCreatedAfter()},
	}
	listSearchIndexExportsReply, err := s.listSearchIndexExportsFn(ctx, listSearchIndexExportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/ListSearchIndexExports")
	}
	return &reviewsv1.ListProductReviewsResponse{
		ProductReviews: []*reviewsv1.ProductReview{&reviewsv1.ProductReview{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: recalculateReviewModerationsReply.GetReviewModeration().GetAudit().GetUpdatedAt(),
			},
		}},
		Result: &commonv1.OperationResult{
			Status:       recalculateReviewModerationsReply.GetResult().GetStatus(),
			ReferenceIds: listSearchIndexExportsReply.GetResult().GetReferenceIds(),
			Message:      listSearchIndexExportsReply.GetResult().GetMessage(),
		},
	}, nil
}

// BatchGetProductReviews returns several product reviews at once.
func (s *ProductReviewServer) BatchGetProductReviews(ctx context.Context, req *reviewsv1.BatchGetProductReviewsRequest) (*reviewsv1.BatchGetProductReviewsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected BatchGetProductReviews", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "batch get product reviews", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "locale", req.GetContext().GetLocale())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	previewReviewVoteReply, err := s.reviewVoteValidationClient.PreviewReviewVote(ctx, &reviewsv1.PreviewReviewVoteRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.PreviewReviewVoteRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  tenancy.Value(ctx, "locale"),
		},
		ReviewVote: &reviewsv1.ReviewVote{
			Measurements: []*reviewsv1.ReviewVoteMeasurement{&reviewsv1.ReviewVoteMeasurement{ObservedAt: cachedContext.GetDeadline()}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteValidationService/PreviewReviewVote")
	}
	return &reviewsv1.BatchGetProductReviewsResponse{
		ProductReviews: []*reviewsv1.ProductReview{&reviewsv1.ProductReview{
			Counterparty: &customersv1.CustomerRef{
				CustomerId: previewReviewVoteReply.GetReviewVote().GetSubject().GetCustomerId(),
			},
		}},
		Result: &commonv1.OperationResult{
			Details: previewReviewVoteReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// SearchProductReviews lists product reviews a page at a time.
func (s *ProductReviewServer) SearchProductReviews(ctx context.Context, req *reviewsv1.SearchProductReviewsRequest) (*reviewsv1.SearchProductReviewsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "search product reviews", "tenant", req.GetContext().GetTenantId())
	scoreSessionRequest := &identityv1.ScoreSessionRequest{
		Context: req.GetContext(),
		Details: &identityv1.ScoreSessionRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	scoreSessionReply, err := scoreSessionCall(ctx, scoreSessionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionValidationService/ScoreSession")
	}
	return &reviewsv1.SearchProductReviewsResponse{
		ProductReviews: []*reviewsv1.ProductReview{&reviewsv1.ProductReview{
			DueAt: scoreSessionReply.GetSession().GetSla().GetDueAt(),
			Counterparty: &customersv1.CustomerRef{
				DisplayName: scoreSessionReply.GetSession().GetAssignee().GetDisplayName(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// WatchProductReviews streams the changes of product reviews as they happen.
func (s *ProductReviewServer) WatchProductReviews(req *reviewsv1.WatchProductReviewsRequest, stream grpc.ServerStreamingServer[reviewsv1.WatchProductReviewsResponse]) error {
	ctx := stream.Context()
	s.log.InfoContext(ctx, "watch product reviews", "tenant", req.GetContext().GetTenantId())
	listSearchQuerySubscriptionsRequest := &searchv1.ListSearchQuerySubscriptionsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSearchQuerySubscriptionsRequest_Filter{
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			Region:       req.GetFilter().GetRegion(),
			Labels:       req.GetFilter().GetLabels(),
		},
	}
	listSearchQuerySubscriptionsReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*searchv1.ListSearchQuerySubscriptionsResponse, error) {
		return s.searchQueryNotificationClient.ListSearchQuerySubscriptions(ctx, listSearchQuerySubscriptionsRequest)
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/ListSearchQuerySubscriptions")
	}
	if err := stream.Send(&reviewsv1.WatchProductReviewsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  listSearchQuerySubscriptionsReply.GetResult().GetReferenceIds(),
			CorrelationId: listSearchQuerySubscriptionsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		ProductReview: &reviewsv1.ProductReview{
			Counterparty:   &customersv1.CustomerRef{Email: req.GetSubscriber().GetEmail()},
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}); err != nil {
		return err
	}
	return nil
}

// BatchCreateProductReviews takes a stream of product reviews and answers once at the end.
func (s *ProductReviewServer) BatchCreateProductReviews(stream grpc.ClientStreamingServer[reviewsv1.BatchCreateProductReviewsRequest, reviewsv1.BatchCreateProductReviewsResponse]) error {
	ctx := stream.Context()
	var accepted int64
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		s.log.InfoContext(ctx, "batch create product reviews", "tenant", msg.GetContext().GetTenantId())
		if _, err := s.onboardingPlanCalculationClient.QuoteOnboardingPlan(ctx, &onboardingv1.QuoteOnboardingPlanRequest{
			Context: msg.GetContext(),
			Details: &onboardingv1.QuoteOnboardingPlanRequest_Details{
				Destination: &commonv1.PostalAddress{Lines: msg.GetRecord().GetBillingAddress().GetLines()},
			},
		}); err != nil {
			return grpcx.Wrap(err, "acme.onboarding.v1.OnboardingPlanCalculationService/QuoteOnboardingPlan")
		}
		if _, err := s.payRunClient.ClonePayRun(ctx, &payrollv1.ClonePayRunRequest{
			Context: msg.GetContext(),
			PayRun: &payrollv1.PayRun{
				BillingAddress: &commonv1.PostalAddress{
					CountryCode: msg.GetRecord().GetBillingAddress().GetCountryCode(),
				},
			},
		}); err != nil {
			return grpcx.Wrap(err, "acme.payroll.v1.PayRunService/ClonePayRun")
		}
		accepted++
	}
	return stream.SendAndClose(&reviewsv1.BatchCreateProductReviewsResponse{Accepted: accepted})
}

// CloneProductReview creates a product review.
func (s *ProductReviewServer) CloneProductReview(ctx context.Context, req *reviewsv1.CloneProductReviewRequest) (*reviewsv1.CloneProductReviewResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveProductReview(ctx, req.GetContext().GetTenantId(), req.GetProductReview()); err != nil {
		return nil, grpcx.Wrap(err, "save product review")
	}
	s.log.InfoContext(ctx, "clone product review", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductReview().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetProductReview())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded reviewsv1.ProductReview
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	normalizeReviewModerationRequest := &reviewsv1.NormalizeReviewModerationRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: (&decoded).GetAudit().GetUpdatedBy(),
				Etag:      req.GetProductReview().GetAudit().GetEtag(),
			},
		},
	}
	normalizeReviewModerationReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*reviewsv1.NormalizeReviewModerationResponse, error) {
		return s.reviewModerationValidationClient.NormalizeReviewModeration(ctx, normalizeReviewModerationRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationValidationService/NormalizeReviewModeration")
	}
	recentProductReviews.Store(recordKey{req.GetContext().GetTenantId(), req.GetProductReview().GetProductReviewId()}, req.GetProductReview())
	return &reviewsv1.CloneProductReviewResponse{
		Result: &commonv1.OperationResult{
			Message: normalizeReviewModerationReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		ProductReview: &reviewsv1.ProductReview{
			Metadata: req.GetProductReview().GetMetadata(),
			IssuedAt: req.GetProductReview().GetIssuedAt(),
		},
	}, nil
}

// productReviewServerListProductReviewsRecalculateReviewModerationsStep carries the request of one step of
// ListProductReviews and, once it ran, its reply.
type productReviewServerListProductReviewsRecalculateReviewModerationsStep struct {
	request *reviewsv1.RecalculateReviewModerationsRequest
	reply   *reviewsv1.RecalculateReviewModerationsResponse
}

func (s *ProductReviewServer) runListProductReviewsRecalculateReviewModerations(ctx context.Context, step *productReviewServerListProductReviewsRecalculateReviewModerationsStep) error {
	reply, err := s.reviewModerationCalculationClient.RecalculateReviewModerations(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
