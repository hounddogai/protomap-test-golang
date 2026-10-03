package checkout

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// ShippingQuoteQueryServer implements `acme.checkout.v1.ShippingQuoteQueryService`.
// Answers read-only questions about shipping quotes, for screens and reports.
type ShippingQuoteQueryServer struct {
	checkoutv1.UnimplementedShippingQuoteQueryServiceServer
	store                             *Store
	log                               *slog.Logger
	paymentIntentValidationClient     checkoutv1.PaymentIntentValidationServiceClient
	riskAssessmentCalculationPort     riskAssessmentCalculationPort
	conn                              *grpc.ClientConn
	explainTrackingEventCalculationFn func(context.Context, *shippingv1.ExplainTrackingEventCalculationRequest, ...grpc.CallOption) (*shippingv1.ExplainTrackingEventCalculationResponse, error)
	documentQueryClient               documentsv1.DocumentQueryServiceClient
	synonymSyncClient                 searchv1.SynonymSyncServiceClient
	synonymSyncClientFallback         searchv1.SynonymSyncServiceClient
	reviewVoteValidationOnce          sync.Once
	reviewVoteValidationCached        reviewsv1.ReviewVoteValidationServiceClient
	productReviewClient               reviewsv1.ProductReviewServiceClient
}

// NewShippingQuoteQueryServer wires a ShippingQuoteQueryServer.
func NewShippingQuoteQueryServer(deps platform.Deps) *ShippingQuoteQueryServer {
	trackingEventCalculation := shippingv1.NewTrackingEventCalculationServiceClient(deps.Conn)
	return &ShippingQuoteQueryServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.checkout.v1.ShippingQuoteQueryService"),
		paymentIntentValidationClient:     checkoutv1.NewPaymentIntentValidationServiceClient(deps.Conn),
		riskAssessmentCalculationPort:     newRiskAssessmentCalculationPort(deps),
		conn:                              deps.Conn,
		explainTrackingEventCalculationFn: trackingEventCalculation.ExplainTrackingEventCalculation,
		documentQueryClient:               documentsv1.NewDocumentQueryServiceClient(deps.Conn),
		synonymSyncClient:                 searchv1.NewSynonymSyncServiceClient(deps.Conn),
		synonymSyncClientFallback:         searchv1.NewSynonymSyncServiceClient(deps.Conn),
		productReviewClient:               reviewsv1.NewProductReviewServiceClient(deps.Conn),
	}
}

// LookupShippingQuote returns a shipping quote.
func (s *ShippingQuoteQueryServer) LookupShippingQuote(ctx context.Context, req *checkoutv1.LookupShippingQuoteRequest) (*checkoutv1.LookupShippingQuoteResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shipping_quote", req.GetShippingQuoteId(), "lookup_shipping_quote"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup shipping quote", "tenant", req.GetContext().GetTenantId())
	return &checkoutv1.LookupShippingQuoteResponse{
		ShippingQuote: &checkoutv1.ShippingQuote{ShippingQuoteId: req.GetShippingQuoteId()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BrowseShippingQuotes lists shipping quotes a page at a time.
func (s *ShippingQuoteQueryServer) BrowseShippingQuotes(ctx context.Context, req *checkoutv1.BrowseShippingQuotesRequest) (*checkoutv1.BrowseShippingQuotesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "browse shipping quotes", "tenant", req.GetContext().GetTenantId())
	normalizePaymentIntentRequest := &checkoutv1.NormalizePaymentIntentRequest{}
	normalizePaymentIntentRequest.Context = req.GetContext()
	normalizePaymentIntentRequest.Details = &checkoutv1.NormalizePaymentIntentRequest_Details{
		Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
	}
	normalizePaymentIntentRequest.PaymentIntent = &checkoutv1.PaymentIntent{
		Assignee: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
	}
	normalizePaymentIntentReply, err := s.paymentIntentValidationClient.NormalizePaymentIntent(ctx, normalizePaymentIntentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.PaymentIntentValidationService/NormalizePaymentIntent")
	}
	explainRiskAssessmentCalculationRequest := &fraudv1.ExplainRiskAssessmentCalculationRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: normalizePaymentIntentReply.GetPaymentIntent().GetAudit().GetUpdatedAt(),
			},
			Assignee: &identityv1.UserRef{
				Email: normalizePaymentIntentReply.GetPaymentIntent().GetRequester().GetEmail(),
			},
		},
	}
	explainRiskAssessmentCalculationReply, err := s.riskAssessmentCalculationPort.ExplainRiskAssessmentCalculation(ctx, explainRiskAssessmentCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/ExplainRiskAssessmentCalculation")
	}
	return &checkoutv1.BrowseShippingQuotesResponse{
		ShippingQuotes: []*checkoutv1.ShippingQuote{&checkoutv1.ShippingQuote{
			Audit: &commonv1.AuditInfo{
				CreatedBy: normalizePaymentIntentReply.GetPaymentIntent().GetAudit().GetCreatedBy(),
				Etag:      explainRiskAssessmentCalculationReply.GetRiskAssessment().GetAudit().GetEtag(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// FindShippingQuotes lists shipping quotes a page at a time.
func (s *ShippingQuoteQueryServer) FindShippingQuotes(ctx context.Context, req *checkoutv1.FindShippingQuotesRequest) (*checkoutv1.FindShippingQuotesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "find shipping quotes", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetFilter()).(*checkoutv1.FindShippingQuotesRequest_Filter)
	scorePaymentIntentRequest := &checkoutv1.ScorePaymentIntentRequest{
		Context: req.GetContext(),
		Details: &checkoutv1.ScorePaymentIntentRequest_Details{
			Destination: &commonv1.PostalAddress{Region: cloned.GetRegion()},
		},
		PaymentIntent: &checkoutv1.PaymentIntent{
			Assignee: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Comments: []*checkoutv1.PaymentIntentComment{&checkoutv1.PaymentIntentComment{CreatedAt: req.GetFilter().GetCreatedAfter()}},
		},
	}
	scorePaymentIntentReply, err := grpcx.NewClient(s.conn, checkoutv1.NewPaymentIntentValidationServiceClient).ScorePaymentIntent(ctx, scorePaymentIntentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.PaymentIntentValidationService/ScorePaymentIntent")
	}
	return &checkoutv1.FindShippingQuotesResponse{
		ShippingQuotes: []*checkoutv1.ShippingQuote{&checkoutv1.ShippingQuote{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: scorePaymentIntentReply.GetPaymentIntent().GetAudit().GetUpdatedBy(),
			},
			Counterparty: &customersv1.CustomerRef{
				Email: scorePaymentIntentReply.GetPaymentIntent().GetRequester().GetEmail(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ResolveShippingQuotes returns several shipping quotes at once.
func (s *ShippingQuoteQueryServer) ResolveShippingQuotes(ctx context.Context, req *checkoutv1.ResolveShippingQuotesRequest) (*checkoutv1.ResolveShippingQuotesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ResolveShippingQuotes", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "resolve shipping quotes", "tenant", req.GetContext().GetTenantId())
	return &checkoutv1.ResolveShippingQuotesResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ExportShippingQuotes streams every matching shipping quote.
func (s *ShippingQuoteQueryServer) ExportShippingQuotes(req *checkoutv1.ExportShippingQuotesRequest, stream grpc.ServerStreamingServer[checkoutv1.ExportShippingQuotesResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExportShippingQuotes", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "export shipping quotes", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "pack payload")
	}
	explainTrackingEventCalculationRequest := &shippingv1.ExplainTrackingEventCalculationRequest{
		Context: req.GetContext(),
		Details: &shippingv1.ExplainTrackingEventCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		TrackingEvent: &shippingv1.TrackingEvent{
			Measurements: []*shippingv1.TrackingEventMeasurement{&shippingv1.TrackingEventMeasurement{ObservedAt: req.GetFilter().GetCreatedAfter()}},
			Detail:       packed,
		},
	}
	explainTrackingEventCalculationReply, err := s.explainTrackingEventCalculationFn(ctx, explainTrackingEventCalculationRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.shipping.v1.TrackingEventCalculationService/ExplainTrackingEventCalculation")
	}
	summarizeDocumentsRequest := buildSummarizeDocumentsRequestForExportShippingQuotes(req, explainTrackingEventCalculationReply)
	summarizeDocumentsReply, err := s.documentQueryClient.SummarizeDocuments(ctx, summarizeDocumentsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.documents.v1.DocumentQueryService/SummarizeDocuments")
	}
	if err := stream.Send(&checkoutv1.ExportShippingQuotesResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: explainTrackingEventCalculationReply.GetResult().GetCorrelationId(),
			Status:        summarizeDocumentsReply.GetResult().GetStatus(),
			ReferenceIds:  summarizeDocumentsReply.GetResult().GetReferenceIds(),
		},
		ShippingQuote: &checkoutv1.ShippingQuote{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizeShippingQuotes summarizes a shipping quote.
func (s *ShippingQuoteQueryServer) SummarizeShippingQuotes(ctx context.Context, req *checkoutv1.SummarizeShippingQuotesRequest) (*checkoutv1.SummarizeShippingQuotesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shipping_quote", req.GetShippingQuoteId(), "summarize_shipping_quotes"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize shipping quotes", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *searchv1.PushSynonymRequest, ...grpc.CallOption) (*searchv1.PushSynonymResponse, error){
		"primary":  s.synonymSyncClient.PushSynonym,
		"fallback": s.synonymSyncClientFallback.PushSynonym,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	pushSynonymRequest := &searchv1.PushSynonymRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
	}
	pushSynonymReply, err := route(ctx, pushSynonymRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/PushSynonym")
	}
	validateReviewVoteRequest := &reviewsv1.ValidateReviewVoteRequest{
		Context: req.GetContext(),
		Reason:  pushSynonymReply.GetSynonym().GetBlock().GetReason(),
	}
	validateReviewVoteReply, err := s.reviewVoteValidationLazy().ValidateReviewVote(ctx, validateReviewVoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteValidationService/ValidateReviewVote")
	}
	return &checkoutv1.SummarizeShippingQuotesResponse{
		Result: &commonv1.OperationResult{
			Status:  pushSynonymReply.GetResult().GetStatus(),
			Message: validateReviewVoteReply.GetOutcome().GetMessage(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestShippingQuotes lists shipping quotes a page at a time.
func (s *ShippingQuoteQueryServer) SuggestShippingQuotes(ctx context.Context, req *checkoutv1.SuggestShippingQuotesRequest) (*checkoutv1.SuggestShippingQuotesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuggestShippingQuotes", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "suggest shipping quotes", "tenant", req.GetContext().GetTenantId())
	filterCache := cache.New[string, *checkoutv1.SuggestShippingQuotesRequest_Filter](1)
	filterCache.Put(req.GetContext().GetTraceId(), req.GetFilter())
	cachedFilter, _ := filterCache.Get(req.GetContext().GetTraceId())
	watchProductReviewsStream, err := s.productReviewClient.WatchProductReviews(ctx, &reviewsv1.WatchProductReviewsRequest{
		Context: req.GetContext(),
		Filter: &reviewsv1.WatchProductReviewsRequest_Filter{
			Query:   req.GetFilter().GetQuery(),
			Labels:  req.GetFilter().GetLabels(),
			Region:  cachedFilter.GetRegion(),
			OwnerId: req.GetFilter().GetOwnerId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewService/WatchProductReviews")
	}
	for {
		received, err := watchProductReviewsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewService/WatchProductReviews")
		}
		_ = received
	}
	return &checkoutv1.SuggestShippingQuotesResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// PeekShippingQuote returns a shipping quote.
func (s *ShippingQuoteQueryServer) PeekShippingQuote(ctx context.Context, req *checkoutv1.PeekShippingQuoteRequest) (*checkoutv1.PeekShippingQuoteResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shipping_quote", req.GetShippingQuoteId(), "peek_shipping_quote"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek shipping quote", "tenant", req.GetContext().GetTenantId())
	return &checkoutv1.PeekShippingQuoteResponse{
		ShippingQuote: &checkoutv1.ShippingQuote{ShippingQuoteId: req.GetShippingQuoteId()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// reviewVoteValidationLazy dials acme.reviews.v1.ReviewVoteValidationService the first time a handler needs it.
func (s *ShippingQuoteQueryServer) reviewVoteValidationLazy() reviewsv1.ReviewVoteValidationServiceClient {
	s.reviewVoteValidationOnce.Do(func() { s.reviewVoteValidationCached = reviewsv1.NewReviewVoteValidationServiceClient(s.conn) })
	return s.reviewVoteValidationCached
}
