package checkout

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	patientsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/patients/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CheckoutSessionServer implements `acme.checkout.v1.CheckoutSessionService`.
// Manages checkout sessions: creates, reads, changes, and lists them.
type CheckoutSessionServer struct {
	*checkoutSessionHandlers
}

// checkoutSessionHandlers holds the handlers of CheckoutSessionServer, with what they use.
type checkoutSessionHandlers struct {
	checkoutv1.UnimplementedCheckoutSessionServiceServer
	store                         *Store
	log                           *slog.Logger
	checkoutSessionQueryClient    checkoutv1.CheckoutSessionQueryServiceClient
	tierCalculationClient         loyaltyv1.TierCalculationServiceClient
	conn                          *grpc.ClientConn
	clients                       *registry.Registry
	productReviewClient           reviewsv1.ProductReviewServiceClient
	searchQueryHistoryClient      searchv1.SearchQueryHistoryServiceClient
	accessPolicyValidationClient  authorizationv1.AccessPolicyValidationServiceClient
	shippingQuoteQueryClient      checkoutv1.ShippingQuoteQueryServiceClient
	userValidationClient          identityv1.UserValidationServiceClient
	paymentIntentValidationClient checkoutv1.PaymentIntentValidationServiceClient
	metricSeriesQueryClient       metricsv1.MetricSeriesQueryServiceClient
}

// NewCheckoutSessionServer wires a CheckoutSessionServer.
func NewCheckoutSessionServer(deps platform.Deps) *CheckoutSessionServer {
	return &CheckoutSessionServer{checkoutSessionHandlers: &checkoutSessionHandlers{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.checkout.v1.CheckoutSessionService"),
		checkoutSessionQueryClient:    checkoutv1.NewCheckoutSessionQueryServiceClient(deps.Conn),
		tierCalculationClient:         loyaltyv1.NewTierCalculationServiceClient(deps.Conn),
		conn:                          deps.Conn,
		clients:                       deps.Clients,
		productReviewClient:           reviewsv1.NewProductReviewServiceClient(deps.Conn),
		searchQueryHistoryClient:      searchv1.NewSearchQueryHistoryServiceClient(deps.Conn),
		accessPolicyValidationClient:  authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn),
		shippingQuoteQueryClient:      checkoutv1.NewShippingQuoteQueryServiceClient(deps.Conn),
		userValidationClient:          identityv1.NewUserValidationServiceClient(deps.Conn),
		paymentIntentValidationClient: checkoutv1.NewPaymentIntentValidationServiceClient(deps.Conn),
		metricSeriesQueryClient:       metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn),
	}}
}

// CreateCheckoutSession creates a checkout session.
func (s *checkoutSessionHandlers) CreateCheckoutSession(ctx context.Context, req *checkoutv1.CreateCheckoutSessionRequest) (*checkoutv1.CreateCheckoutSessionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCheckoutSession(ctx, req.GetContext().GetTenantId(), req.GetCheckoutSession()); err != nil {
		return nil, grpcx.Wrap(err, "save checkout session")
	}
	s.log.InfoContext(ctx, "create checkout session", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCheckoutSession().GetAudit().GetEtag())
	resolveCheckoutSessionsRequest := &checkoutv1.ResolveCheckoutSessionsRequest{Context: req.GetContext()}
	resolveCheckoutSessionsReply, err := grpcx.Call(ctx, s.checkoutSessionQueryClient.ResolveCheckoutSessions, resolveCheckoutSessionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionQueryService/ResolveCheckoutSessions")
	}
	compareTierOptionsReply, err := s.tierCalculationClient.CompareTierOptions(ctx, &loyaltyv1.CompareTierOptionsRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.CompareTierOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: req.GetCheckoutSession().GetBillingAddress().GetRecipient(),
				Locality:  req.GetCheckoutSession().GetBillingAddress().GetLocality(),
			},
		},
		Tier: &loyaltyv1.Tier{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetCheckoutSession().GetAudit().GetUpdatedAt(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierCalculationService/CompareTierOptions")
	}
	listProductReviewsRequest := &reviewsv1.ListProductReviewsRequest{
		Context: req.GetContext(),
		Filter: &reviewsv1.ListProductReviewsRequest_Filter{
			Region: req.GetCheckoutSession().GetBillingAddress().GetRegion(),
		},
	}
	listProductReviewsReply := new(reviewsv1.ListProductReviewsResponse)
	if err := grpcx.Invoke(ctx, s.conn, reviewsv1.ProductReviewService_ListProductReviews_FullMethodName, listProductReviewsRequest, listProductReviewsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewService/ListProductReviews")
	}
	patientQuery, ok := s.clients.Get("acme.patients.v1.PatientQueryService").(patientsv1.PatientQueryServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.patients.v1.PatientQueryService")
	}
	resolvePatientsRequest := &patientsv1.ResolvePatientsRequest{Context: req.GetContext()}
	resolvePatientsReply, err := patientQuery.ResolvePatients(ctx, resolvePatientsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.patients.v1.PatientQueryService/ResolvePatients")
	}
	return &checkoutv1.CreateCheckoutSessionResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: resolveCheckoutSessionsReply.GetResult().GetCorrelationId(),
			Message:       resolveCheckoutSessionsReply.GetResult().GetMessage(),
			ReferenceIds:  listProductReviewsReply.GetResult().GetReferenceIds(),
			Status:        resolvePatientsReply.GetResult().GetStatus(),
		},
		CheckoutSession: &checkoutv1.CheckoutSession{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: compareTierOptionsReply.GetTier().GetAudit().GetUpdatedAt(),
			},
			Total:        req.GetCheckoutSession().GetTotal(),
			Counterparty: req.GetCheckoutSession().GetCounterparty(),
		},
	}, nil
}

// GetCheckoutSession returns a checkout session.
func (s *checkoutSessionHandlers) GetCheckoutSession(ctx context.Context, req *checkoutv1.GetCheckoutSessionRequest) (*checkoutv1.GetCheckoutSessionResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "checkout_session", req.GetCheckoutSessionId(), "get_checkout_session"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get checkout session", "tenant", req.GetContext().GetTenantId())
	return &checkoutv1.GetCheckoutSessionResponse{
		CheckoutSession: &checkoutv1.CheckoutSession{CheckoutSessionId: req.GetCheckoutSessionId()},
		Result:          &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// UpdateCheckoutSession changes a checkout session.
func (s *checkoutSessionHandlers) UpdateCheckoutSession(ctx context.Context, req *checkoutv1.UpdateCheckoutSessionRequest) (*checkoutv1.UpdateCheckoutSessionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected UpdateCheckoutSession", "error", err)
		return nil, err
	}
	if err := s.store.SaveCheckoutSession(ctx, req.GetContext().GetTenantId(), req.GetCheckoutSession()); err != nil {
		return nil, grpcx.Wrap(err, "save checkout session")
	}
	s.log.InfoContext(ctx, "update checkout session", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCheckoutSession().GetAudit().GetEtag())
	resolveCheckoutSessionsRequest := &checkoutv1.ResolveCheckoutSessionsRequest{Context: req.GetContext()}
	send := s.resolveCheckoutSessionsSender(resolveCheckoutSessionsRequest)
	resolveCheckoutSessionsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionQueryService/ResolveCheckoutSessions")
	}
	return &checkoutv1.UpdateCheckoutSessionResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: resolveCheckoutSessionsReply.GetResult().GetCorrelationId(),
			Status:        resolveCheckoutSessionsReply.GetResult().GetStatus(),
		},
		CheckoutSession: &checkoutv1.CheckoutSession{
			Status:       req.GetCheckoutSession().GetStatus(),
			Counterparty: req.GetCheckoutSession().GetCounterparty(),
		},
	}, nil
}

// ListCheckoutSessions lists checkout sessions a page at a time.
func (s *checkoutSessionHandlers) ListCheckoutSessions(ctx context.Context, req *checkoutv1.ListCheckoutSessionsRequest) (*checkoutv1.ListCheckoutSessionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list checkout sessions", "tenant", req.GetContext().GetTenantId())
	return &checkoutv1.ListCheckoutSessionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// BatchGetCheckoutSessions returns several checkout sessions at once.
func (s *checkoutSessionHandlers) BatchGetCheckoutSessions(ctx context.Context, req *checkoutv1.BatchGetCheckoutSessionsRequest) (*checkoutv1.BatchGetCheckoutSessionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "batch get checkout sessions", "tenant", req.GetContext().GetTenantId())
	suggestCheckoutSessionsReply, err := s.checkoutSessionQueryClient.SuggestCheckoutSessions(ctx, &checkoutv1.SuggestCheckoutSessionsRequest{
		Context: req.GetContext(),
		Filter: &checkoutv1.SuggestCheckoutSessionsRequest_Filter{
			Labels:       req.GetContext().GetBaggage(),
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionQueryService/SuggestCheckoutSessions")
	}
	return &checkoutv1.BatchGetCheckoutSessionsResponse{
		Result: &commonv1.OperationResult{
			Details: suggestCheckoutSessionsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		PageInfo: &commonv1.PageInfo{
			NextPageToken: suggestCheckoutSessionsReply.GetPageInfo().GetNextPageToken(),
		},
	}, nil
}

// SearchCheckoutSessions lists checkout sessions a page at a time.
func (s *checkoutSessionHandlers) SearchCheckoutSessions(ctx context.Context, req *checkoutv1.SearchCheckoutSessionsRequest) (*checkoutv1.SearchCheckoutSessionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "search checkout sessions", "tenant", req.GetContext().GetTenantId())
	return &checkoutv1.SearchCheckoutSessionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// WatchCheckoutSessions streams the changes of checkout sessions as they happen.
func (s *checkoutSessionHandlers) WatchCheckoutSessions(req *checkoutv1.WatchCheckoutSessionsRequest, stream grpc.ServerStreamingServer[checkoutv1.WatchCheckoutSessionsResponse]) error {
	ctx := stream.Context()
	s.log.InfoContext(ctx, "watch checkout sessions", "tenant", req.GetContext().GetTenantId())
	watchProductReviewsStream, err := s.productReviewClient.WatchProductReviews(ctx, &reviewsv1.WatchProductReviewsRequest{
		Context: req.GetContext(),
		Filter:  &reviewsv1.WatchProductReviewsRequest_Filter{OwnerId: req.GetFilter().GetOwnerId()},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.reviews.v1.ProductReviewService/WatchProductReviews")
	}
	for {
		received, err := watchProductReviewsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return grpcx.Wrap(err, "acme.reviews.v1.ProductReviewService/WatchProductReviews")
		}
		_ = received
	}
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	suggestCheckoutSessionsRequest := &checkoutv1.SuggestCheckoutSessionsRequest{
		Context: req.GetContext(),
		Filter:  &checkoutv1.SuggestCheckoutSessionsRequest_Filter{Labels: labels},
	}
	suggestCheckoutSessionsReply, err := grpcx.Call(ctx, s.checkoutSessionQueryClient.SuggestCheckoutSessions, suggestCheckoutSessionsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionQueryService/SuggestCheckoutSessions")
	}
	recordSearchQueryVersionRequest := buildRecordSearchQueryVersionRequestForWatchCheckoutSessions(req, suggestCheckoutSessionsReply)
	recordSearchQueryVersionReply, err := s.searchQueryHistoryClient.RecordSearchQueryVersion(ctx, recordSearchQueryVersionRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.search.v1.SearchQueryHistoryService/RecordSearchQueryVersion")
	}
	if err := stream.Send(&checkoutv1.WatchCheckoutSessionsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  suggestCheckoutSessionsReply.GetResult().GetReferenceIds(),
			CorrelationId: recordSearchQueryVersionReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		CheckoutSession: &checkoutv1.CheckoutSession{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}); err != nil {
		return err
	}
	return nil
}

// BatchCreateCheckoutSessions takes a stream of checkout sessions and answers once at the end.
func (s *checkoutSessionHandlers) BatchCreateCheckoutSessions(stream grpc.ClientStreamingServer[checkoutv1.BatchCreateCheckoutSessionsRequest, checkoutv1.BatchCreateCheckoutSessionsResponse]) error {
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
		s.log.InfoContext(ctx, "batch create checkout sessions", "tenant", msg.GetContext().GetTenantId())
		jobs := make(chan *checkoutv1.CheckoutSessionLine)
		failures := make(chan error, 4)
		workCtx, stopWork := context.WithCancel(ctx)
		var wg sync.WaitGroup
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for line := range jobs {
					if _, err := s.accessPolicyValidationClient.LintAccessPolicies(workCtx, &authorizationv1.LintAccessPoliciesRequest{
						Context:      msg.GetContext(),
						AccessPolicy: &authorizationv1.AccessPolicy{Description: line.GetDescription()},
					}); err != nil {
						failures <- err
						stopWork()
						return
					}
				}
			}()
		}
		for _, queued := range msg.GetRecord().GetLines() {
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
			return grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/LintAccessPolicies")
		}
		browseShippingQuotesRequest := &checkoutv1.BrowseShippingQuotesRequest{
			Context: msg.GetContext(),
			Filter: &checkoutv1.BrowseShippingQuotesRequest_Filter{
				Region:       msg.GetRecord().GetBillingAddress().GetRegion(),
				CreatedAfter: msg.GetRecord().GetAudit().GetUpdatedAt(),
			},
		}
		if _, err := grpcx.Call(ctx, s.shippingQuoteQueryClient.BrowseShippingQuotes, browseShippingQuotesRequest); err != nil {
			return grpcx.Wrap(err, "acme.checkout.v1.ShippingQuoteQueryService/BrowseShippingQuotes")
		}
		if _, err := s.userValidationClient.GetUserRules(ctx, &identityv1.GetUserRulesRequest{
			Context: msg.GetContext(),
			UserId:  msg.GetContext().GetActor().GetUserId(),
		}); err != nil {
			return grpcx.Wrap(err, "acme.identity.v1.UserValidationService/GetUserRules")
		}
		scorePaymentIntentRequest := &checkoutv1.ScorePaymentIntentRequest{
			Context: msg.GetContext(),
			Details: &checkoutv1.ScorePaymentIntentRequest_Details{
				Destination: &commonv1.PostalAddress{
					Recipient: msg.GetRecord().GetBillingAddress().GetRecipient(),
				},
			},
			PaymentIntent: &checkoutv1.PaymentIntent{
				Assignee: &identityv1.UserRef{
					DisplayName: msg.GetRecord().GetCounterparty().GetDisplayName(),
				},
				Audit: &commonv1.AuditInfo{CreatedBy: msg.GetRecord().GetAudit().GetCreatedBy()},
			},
		}
		if _, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*checkoutv1.ScorePaymentIntentResponse, error) {
			return s.paymentIntentValidationClient.ScorePaymentIntent(ctx, scorePaymentIntentRequest)
		}); err != nil {
			return grpcx.Wrap(err, "acme.checkout.v1.PaymentIntentValidationService/ScorePaymentIntent")
		}
		accepted++
	}
	return stream.SendAndClose(&checkoutv1.BatchCreateCheckoutSessionsResponse{Accepted: accepted})
}

// CloneCheckoutSession creates a checkout session.
func (s *checkoutSessionHandlers) CloneCheckoutSession(ctx context.Context, req *checkoutv1.CloneCheckoutSessionRequest) (*checkoutv1.CloneCheckoutSessionResponse, error) {
	if err := s.store.SaveCheckoutSession(ctx, req.GetContext().GetTenantId(), req.GetCheckoutSession()); err != nil {
		return nil, grpcx.Wrap(err, "save checkout session")
	}
	s.log.InfoContext(ctx, "clone checkout session", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCheckoutSession().GetAudit().GetEtag())
	normalizePaymentIntentRequest := &checkoutv1.NormalizePaymentIntentRequest{
		Context: req.GetContext(),
		PaymentIntent: &checkoutv1.PaymentIntent{
			Origin: &checkoutv1.PaymentIntent_Customer{Customer: &checkoutv1.CheckoutSessionRef{
				DisplayName: req.GetCheckoutSession().GetCounterparty().GetDisplayName(),
				Number:      req.GetCheckoutSession().GetNumber(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetCheckoutSession().GetAudit().GetCreatedBy(),
				Etag:      req.GetCheckoutSession().GetAudit().GetEtag(),
			},
		},
	}
	normalizePaymentIntentReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*checkoutv1.NormalizePaymentIntentResponse, error) {
		return s.paymentIntentValidationClient.NormalizePaymentIntent(ctx, normalizePaymentIntentRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.PaymentIntentValidationService/NormalizePaymentIntent")
	}
	metadata2 := make(map[string]string, len(req.GetCheckoutSession().GetMetadata()))
	for key, value := range req.GetCheckoutSession().GetMetadata() {
		metadata2[key] = value
	}
	summarizeMetricSeriesesRequest := &metricsv1.SummarizeMetricSeriesesRequest{
		Context:     req.GetContext(),
		Annotations: normalizePaymentIntentReply.GetResult().GetDetails(),
		Details:     &metricsv1.SummarizeMetricSeriesesRequest_Details{Filters: metadata2},
	}
	summarizeMetricSeriesesReply, err := grpcx.Call(ctx, s.metricSeriesQueryClient.SummarizeMetricSerieses, summarizeMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/SummarizeMetricSerieses")
	}
	synonymExport, ok := s.clients.Get("acme.search.v1.SynonymExportService").(searchv1.SynonymExportServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.search.v1.SynonymExportService")
	}
	listSynonymExportsRequest := &searchv1.ListSynonymExportsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSynonymExportsRequest_Filter{
			Region:       req.GetCheckoutSession().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetCheckoutSession().GetAudit().GetCreatedAt(),
			OwnerId:      normalizePaymentIntentReply.GetPaymentIntent().GetPaymentIntentId(),
		},
	}
	listSynonymExportsReply, err := synonymExport.ListSynonymExports(ctx, listSynonymExportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/ListSynonymExports")
	}
	return &checkoutv1.CloneCheckoutSessionResponse{
		Result: &commonv1.OperationResult{
			Message:       normalizePaymentIntentReply.GetOutcome().GetMessage(),
			CorrelationId: summarizeMetricSeriesesReply.GetResult().GetCorrelationId(),
			Details:       summarizeMetricSeriesesReply.GetResult().GetDetails(),
			Status:        listSynonymExportsReply.GetResult().GetStatus(),
		},
		CheckoutSession: &checkoutv1.CheckoutSession{
			Counterparty: req.GetCheckoutSession().GetCounterparty(),
			Notes:        req.GetCheckoutSession().GetNotes(),
		},
	}, nil
}

// resolveCheckoutSessionsSender returns a function that sends one request to
// acme.checkout.v1.CheckoutSessionQueryService/ResolveCheckoutSessions.
func (s *checkoutSessionHandlers) resolveCheckoutSessionsSender(request *checkoutv1.ResolveCheckoutSessionsRequest) func(context.Context) (*checkoutv1.ResolveCheckoutSessionsResponse, error) {
	return func(ctx context.Context) (*checkoutv1.ResolveCheckoutSessionsResponse, error) {
		return s.checkoutSessionQueryClient.ResolveCheckoutSessions(ctx, request)
	}
}
