package checkout

import (
	"context"
	"errors"
	"io"
	"log/slog"

	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// CheckoutSessionQueryServer implements `acme.checkout.v1.CheckoutSessionQueryService`.
// Answers read-only questions about checkout sessions, for screens and reports.
type CheckoutSessionQueryServer struct {
	*checkoutSessionQueryHandlers
}

// checkoutSessionQueryHandlers holds the handlers of CheckoutSessionQueryServer, with what they use.
type checkoutSessionQueryHandlers struct {
	checkoutv1.UnimplementedCheckoutSessionQueryServiceServer
	store                         *Store
	log                           *slog.Logger
	searchIndexClient             searchv1.SearchIndexServiceClient
	validatePaymentIntentFn       func(context.Context, *checkoutv1.ValidatePaymentIntentRequest, ...grpc.CallOption) (*checkoutv1.ValidatePaymentIntentResponse, error)
	bus                           *eventbus.Bus
	paymentIntentValidationClient checkoutv1.PaymentIntentValidationServiceClient
	paymentIntentValidationPort   paymentIntentValidationPort
	meteredSynonymHistory         meteredSynonymHistory
	sessionQueryClient            identityv1.SessionQueryServiceClient
}

// NewCheckoutSessionQueryServer wires a CheckoutSessionQueryServer.
func NewCheckoutSessionQueryServer(deps platform.Deps) *CheckoutSessionQueryServer {
	paymentIntentValidation := checkoutv1.NewPaymentIntentValidationServiceClient(deps.Conn)
	return &CheckoutSessionQueryServer{checkoutSessionQueryHandlers: &checkoutSessionQueryHandlers{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.checkout.v1.CheckoutSessionQueryService"),
		searchIndexClient:             searchv1.NewSearchIndexServiceClient(deps.Conn),
		validatePaymentIntentFn:       paymentIntentValidation.ValidatePaymentIntent,
		bus:                           deps.Bus,
		paymentIntentValidationClient: checkoutv1.NewPaymentIntentValidationServiceClient(deps.Conn),
		paymentIntentValidationPort:   newPaymentIntentValidationPort(deps),
		meteredSynonymHistory:         meteredSynonymHistory{SynonymHistoryServiceClient: searchv1.NewSynonymHistoryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		sessionQueryClient:            identityv1.NewSessionQueryServiceClient(deps.Conn),
	}}
}

// LookupCheckoutSession returns a checkout session.
func (s *checkoutSessionQueryHandlers) LookupCheckoutSession(ctx context.Context, req *checkoutv1.LookupCheckoutSessionRequest) (*checkoutv1.LookupCheckoutSessionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LookupCheckoutSession", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "checkout_session", req.GetCheckoutSessionId(), "lookup_checkout_session"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup checkout session", "tenant", req.GetContext().GetTenantId())
	searchSearchIndexesReply, err := s.searchIndexClient.SearchSearchIndexes(ctx, &searchv1.SearchSearchIndexesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.SearchSearchIndexesRequest_Filter{
			OwnerId:      req.GetCheckoutSessionId(),
			Labels:       req.GetContext().GetBaggage(),
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexService/SearchSearchIndexes")
	}
	validatePaymentIntentRequest := &checkoutv1.ValidatePaymentIntentRequest{
		Context: req.GetContext(),
		PaymentIntent: &checkoutv1.PaymentIntent{
			Assignee: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Origin:   &checkoutv1.PaymentIntent_Customer{Customer: &checkoutv1.CheckoutSessionRef{CheckoutSessionId: req.GetCheckoutSessionId()}},
		},
	}
	validatePaymentIntentReply, err := s.validatePaymentIntentFn(ctx, validatePaymentIntentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.PaymentIntentValidationService/ValidatePaymentIntent")
	}
	return &checkoutv1.LookupCheckoutSessionResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: searchSearchIndexesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		CheckoutSession: &checkoutv1.CheckoutSession{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: validatePaymentIntentReply.GetPaymentIntent().GetAudit().GetUpdatedAt(),
			},
			Number:            validatePaymentIntentReply.GetPaymentIntent().GetCustomer().GetNumber(),
			CheckoutSessionId: req.GetCheckoutSessionId(),
		},
	}, nil
}

// BrowseCheckoutSessions lists checkout sessions a page at a time.
func (s *checkoutSessionQueryHandlers) BrowseCheckoutSessions(ctx context.Context, req *checkoutv1.BrowseCheckoutSessionsRequest) (*checkoutv1.BrowseCheckoutSessionsResponse, error) {
	s.log.InfoContext(ctx, "browse checkout sessions", "tenant", req.GetContext().GetTenantId())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "checkout.browse_checkout_sessions.simulate_seller_payout_schedule",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"seller_payout_schedule.location.region":            req.GetFilter().GetRegion(),
			"seller_payout_schedule.participants.contact.email": req.GetContext().GetActor().GetEmail(),
			"seller_payout_schedule.organizer.user_id":          req.GetContext().GetActor().GetUserId(),
			"context.tenant_id":                                 req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &checkoutv1.BrowseCheckoutSessionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// FindCheckoutSessions lists checkout sessions a page at a time.
func (s *checkoutSessionQueryHandlers) FindCheckoutSessions(ctx context.Context, req *checkoutv1.FindCheckoutSessionsRequest) (*checkoutv1.FindCheckoutSessionsResponse, error) {
	s.log.InfoContext(ctx, "find checkout sessions", "tenant", req.GetContext().GetTenantId())
	return &checkoutv1.FindCheckoutSessionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// CountCheckoutSessions counts checkout sessions by status.
func (s *checkoutSessionQueryHandlers) CountCheckoutSessions(ctx context.Context, req *checkoutv1.CountCheckoutSessionsRequest) (*checkoutv1.CountCheckoutSessionsResponse, error) {
	s.log.InfoContext(ctx, "count checkout sessions", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "region", req.GetFilter().GetRegion())
	scorePaymentIntentRequest := &checkoutv1.ScorePaymentIntentRequest{
		Context: req.GetContext(),
		Details: &checkoutv1.ScorePaymentIntentRequest_Details{
			Destination: &commonv1.PostalAddress{Region: tenancy.Value(ctx, "region")},
		},
		PaymentIntent: &checkoutv1.PaymentIntent{
			Assignee:  &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Requester: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	scorePaymentIntentReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*checkoutv1.ScorePaymentIntentResponse, error) {
		return s.paymentIntentValidationClient.ScorePaymentIntent(ctx, scorePaymentIntentRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.PaymentIntentValidationService/ScorePaymentIntent")
	}
	return &checkoutv1.CountCheckoutSessionsResponse{
		Result: &commonv1.OperationResult{
			Message: scorePaymentIntentReply.GetResult().GetMessage(),
			Status:  scorePaymentIntentReply.GetResult().GetStatus(),
		},
	}, nil
}

// ResolveCheckoutSessions returns several checkout sessions at once.
func (s *checkoutSessionQueryHandlers) ResolveCheckoutSessions(ctx context.Context, req *checkoutv1.ResolveCheckoutSessionsRequest) (*checkoutv1.ResolveCheckoutSessionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "resolve checkout sessions", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	lintPaymentIntentsRequest := &checkoutv1.LintPaymentIntentsRequest{
		Context: req.GetContext(),
		PaymentIntent: &checkoutv1.PaymentIntent{
			Assignee: &identityv1.UserRef{UserId: cloned.GetActor().GetUserId()},
		},
	}
	lintPaymentIntentsReply, err := grpcx.Call(ctx, s.paymentIntentValidationClient.LintPaymentIntents, lintPaymentIntentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.PaymentIntentValidationService/LintPaymentIntents")
	}
	return &checkoutv1.ResolveCheckoutSessionsResponse{
		CheckoutSessions: []*checkoutv1.CheckoutSession{&checkoutv1.CheckoutSession{
			Counterparty: &checkoutv1.ShippingQuoteRef{
				Number: lintPaymentIntentsReply.GetPaymentIntent().GetCustomer().GetNumber(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SummarizeCheckoutSessions summarizes a checkout session.
func (s *checkoutSessionQueryHandlers) SummarizeCheckoutSessions(ctx context.Context, req *checkoutv1.SummarizeCheckoutSessionsRequest) (*checkoutv1.SummarizeCheckoutSessionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "checkout_session", req.GetCheckoutSessionId(), "summarize_checkout_sessions"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize checkout sessions", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	scorePaymentIntentRequest := &checkoutv1.ScorePaymentIntentRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		PaymentIntent: &checkoutv1.PaymentIntent{
			Origin:   &checkoutv1.PaymentIntent_Customer{Customer: &checkoutv1.CheckoutSessionRef{CheckoutSessionId: req.GetCheckoutSessionId()}},
			Assignee: &identityv1.UserRef{UserId: (&decoded).GetActor().GetUserId()},
		},
	}
	scorePaymentIntentReply, err := s.paymentIntentValidationPort.ScorePaymentIntent(ctx, scorePaymentIntentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.PaymentIntentValidationService/ScorePaymentIntent")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := checkoutSessionQueryHandlersCreateSearchIndexSteps[mode]
	createSearchIndexRequest := &searchv1.CreateSearchIndexRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: scorePaymentIntentReply.GetPaymentIntent().GetAudit().GetUpdatedBy(),
			},
		},
	}
	createSearchIndexReply, err := run(s, ctx, createSearchIndexRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexService/CreateSearchIndex")
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	annotateSynonymVersionRequest := &searchv1.AnnotateSynonymVersionRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: createSearchIndexReply.GetSearchIndex().GetAudit().GetUpdatedAt(),
				CreatedBy: createSearchIndexReply.GetSearchIndex().GetAudit().GetCreatedBy(),
			},
			Owner: &identityv1.UserRef{
				Email:  scorePaymentIntentReply.GetPaymentIntent().GetAssignee().GetEmail(),
				UserId: scorePaymentIntentReply.GetPaymentIntent().GetAssignee().GetUserId(),
			},
		},
		Details: &searchv1.AnnotateSynonymVersionRequest_Details{Snapshot: snapshot},
	}
	annotateSynonymVersionReply, err := s.meteredSynonymHistory.AnnotateSynonymVersion(ctx, annotateSynonymVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymHistoryService/AnnotateSynonymVersion")
	}
	exportSessionsStream, err := s.sessionQueryClient.ExportSessions(ctx, &identityv1.ExportSessionsRequest{
		Context: req.GetContext(),
		Format:  req.GetDetails().GetFormat(),
		Filter: &identityv1.ExportSessionsRequest_Filter{
			Region:       createSearchIndexReply.GetSearchIndex().GetAddress().GetRegion(),
			CreatedAfter: annotateSynonymVersionReply.GetSynonym().GetAudit().GetUpdatedAt(),
			OwnerId:      scorePaymentIntentReply.GetPaymentIntent().GetSla().GetPolicyId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/ExportSessions")
	}
	for {
		received, err := exportSessionsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/ExportSessions")
		}
		_ = received
	}
	return &checkoutv1.SummarizeCheckoutSessionsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: scorePaymentIntentReply.GetResult().GetReferenceIds(),
			Status:       createSearchIndexReply.GetResult().GetStatus(),
			Message:      annotateSynonymVersionReply.GetResult().GetMessage(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestCheckoutSessions lists checkout sessions a page at a time.
func (s *checkoutSessionQueryHandlers) SuggestCheckoutSessions(ctx context.Context, req *checkoutv1.SuggestCheckoutSessionsRequest) (*checkoutv1.SuggestCheckoutSessionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "suggest checkout sessions", "tenant", req.GetContext().GetTenantId())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := checkoutSessionQueryHandlersExplainPaymentIntentDecisionSteps[mode]
	ctx = tenancy.With(ctx, "user_id", req.GetContext().GetActor().GetUserId())
	explainPaymentIntentDecisionRequest := &checkoutv1.ExplainPaymentIntentDecisionRequest{
		Context: req.GetContext(),
		PaymentIntent: &checkoutv1.PaymentIntent{
			Requester: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Assignee:  &identityv1.UserRef{UserId: tenancy.Value(ctx, "user_id")},
		},
		Details: &checkoutv1.ExplainPaymentIntentDecisionRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	explainPaymentIntentDecisionReply, err := run(s, ctx, explainPaymentIntentDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.PaymentIntentValidationService/ExplainPaymentIntentDecision")
	}
	previewPaymentIntentRequest := &checkoutv1.PreviewPaymentIntentRequest{
		Context: req.GetContext(),
		PaymentIntent: &checkoutv1.PaymentIntent{
			Sla: &checkoutv1.PaymentIntentSla{
				PausedFor: explainPaymentIntentDecisionReply.GetPaymentIntent().GetSla().GetPausedFor(),
			},
			Requester: &commonv1.ContactInfo{
				PreferredChannel: explainPaymentIntentDecisionReply.GetPaymentIntent().GetRequester().GetPreferredChannel(),
			},
			Origin: &checkoutv1.PaymentIntent_Channel{Channel: &checkoutv1.PaymentIntentChannelOrigin{
				Address: explainPaymentIntentDecisionReply.GetPaymentIntent().GetChannel().GetAddress(),
			}},
		},
	}
	send := s.previewPaymentIntentSender(previewPaymentIntentRequest)
	previewPaymentIntentReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.PaymentIntentValidationService/PreviewPaymentIntent")
	}
	return &checkoutv1.SuggestCheckoutSessionsResponse{
		CheckoutSessions: []*checkoutv1.CheckoutSession{&checkoutv1.CheckoutSession{
			Audit: &commonv1.AuditInfo{
				Etag: explainPaymentIntentDecisionReply.GetPaymentIntent().GetAudit().GetEtag(),
			},
			Counterparty: &checkoutv1.ShippingQuoteRef{
				DisplayName: previewPaymentIntentReply.GetPaymentIntent().GetAssignee().GetDisplayName(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// checkoutSessionQueryHandlersCreateSearchIndexSteps are the ways to run
// acme.search.v1.SearchIndexService/CreateSearchIndex, by name.
var checkoutSessionQueryHandlersCreateSearchIndexSteps = map[string]func(*checkoutSessionQueryHandlers, context.Context, *searchv1.CreateSearchIndexRequest) (*searchv1.CreateSearchIndexResponse, error){
	"default": (*checkoutSessionQueryHandlers).createSearchIndexStep,
	"audited": (*checkoutSessionQueryHandlers).createSearchIndexAuditedStep,
}

func (s *checkoutSessionQueryHandlers) createSearchIndexStep(ctx context.Context, request *searchv1.CreateSearchIndexRequest) (*searchv1.CreateSearchIndexResponse, error) {
	return s.searchIndexClient.CreateSearchIndex(ctx, request)
}

func (s *checkoutSessionQueryHandlers) createSearchIndexAuditedStep(ctx context.Context, request *searchv1.CreateSearchIndexRequest) (*searchv1.CreateSearchIndexResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.search.v1.SearchIndexService/CreateSearchIndex")
	return s.searchIndexClient.CreateSearchIndex(ctx, request)
}

// checkoutSessionQueryHandlersExplainPaymentIntentDecisionSteps are the ways to run
// acme.checkout.v1.PaymentIntentValidationService/ExplainPaymentIntentDecision, by name.
var checkoutSessionQueryHandlersExplainPaymentIntentDecisionSteps = map[string]func(*checkoutSessionQueryHandlers, context.Context, *checkoutv1.ExplainPaymentIntentDecisionRequest) (*checkoutv1.ExplainPaymentIntentDecisionResponse, error){
	"default": (*checkoutSessionQueryHandlers).explainPaymentIntentDecisionStep,
	"audited": (*checkoutSessionQueryHandlers).explainPaymentIntentDecisionAuditedStep,
}

func (s *checkoutSessionQueryHandlers) explainPaymentIntentDecisionStep(ctx context.Context, request *checkoutv1.ExplainPaymentIntentDecisionRequest) (*checkoutv1.ExplainPaymentIntentDecisionResponse, error) {
	return s.paymentIntentValidationClient.ExplainPaymentIntentDecision(ctx, request)
}

func (s *checkoutSessionQueryHandlers) explainPaymentIntentDecisionAuditedStep(ctx context.Context, request *checkoutv1.ExplainPaymentIntentDecisionRequest) (*checkoutv1.ExplainPaymentIntentDecisionResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.checkout.v1.PaymentIntentValidationService/ExplainPaymentIntentDecision")
	return s.paymentIntentValidationClient.ExplainPaymentIntentDecision(ctx, request)
}

// previewPaymentIntentSender returns a function that sends one request to
// acme.checkout.v1.PaymentIntentValidationService/PreviewPaymentIntent.
func (s *checkoutSessionQueryHandlers) previewPaymentIntentSender(request *checkoutv1.PreviewPaymentIntentRequest) func(context.Context) (*checkoutv1.PreviewPaymentIntentResponse, error) {
	return func(ctx context.Context) (*checkoutv1.PreviewPaymentIntentResponse, error) {
		return s.paymentIntentValidationClient.PreviewPaymentIntent(ctx, request)
	}
}
