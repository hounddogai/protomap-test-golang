package reviews

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	pipelinesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pipelines/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ProductReviewValidationServer implements `acme.reviews.v1.ProductReviewValidationService`.
// Checks product reviews against the rules before anything acts on them.
type ProductReviewValidationServer struct {
	reviewsv1.UnimplementedProductReviewValidationServiceServer
	store                                     *Store
	log                                       *slog.Logger
	reviewModerationCalculationClient         reviewsv1.ReviewModerationCalculationServiceClient
	documentValidationClient                  documentsv1.DocumentValidationServiceClient
	meteredPipelineCalculation                meteredPipelineCalculation
	notificationClient                        notificationsv1.NotificationServiceClient
	lintNotificationsFn                       func(context.Context, *notificationsv1.LintNotificationsRequest, ...grpc.CallOption) (*notificationsv1.LintNotificationsResponse, error)
	conn                                      *grpc.ClientConn
	reviewModerationCalculationClientFallback reviewsv1.ReviewModerationCalculationServiceClient
}

// NewProductReviewValidationServer wires a ProductReviewValidationServer.
func NewProductReviewValidationServer(deps platform.Deps) *ProductReviewValidationServer {
	notificationValidation := notificationsv1.NewNotificationValidationServiceClient(deps.Conn)
	return &ProductReviewValidationServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.reviews.v1.ProductReviewValidationService"),
		reviewModerationCalculationClient: reviewsv1.NewReviewModerationCalculationServiceClient(deps.Conn),
		documentValidationClient:          documentsv1.NewDocumentValidationServiceClient(deps.Conn),
		meteredPipelineCalculation:        meteredPipelineCalculation{PipelineCalculationServiceClient: pipelinesv1.NewPipelineCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		notificationClient:                notificationsv1.NewNotificationServiceClient(deps.Conn),
		lintNotificationsFn:               notificationValidation.LintNotifications,
		conn:                              deps.Conn,
		reviewModerationCalculationClientFallback: reviewsv1.NewReviewModerationCalculationServiceClient(deps.Conn),
	}
}

// ValidateProductReview validates a product review.
func (s *ProductReviewValidationServer) ValidateProductReview(ctx context.Context, req *reviewsv1.ValidateProductReviewRequest) (*reviewsv1.ValidateProductReviewResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveProductReview(ctx, req.GetContext().GetTenantId(), req.GetProductReview()); err != nil {
		return nil, grpcx.Wrap(err, "save product review")
	}
	s.log.InfoContext(ctx, "validate product review", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductReview().GetAudit().GetEtag())
	calculateReviewModerationRequest := &reviewsv1.CalculateReviewModerationRequest{}
	calculateReviewModerationRequest.Context = req.GetContext()
	calculateReviewModerationRequest.ReviewModeration = &reviewsv1.ReviewModeration{
		Audit: &commonv1.AuditInfo{Etag: req.GetProductReview().GetAudit().GetEtag()},
		Requester: &commonv1.ContactInfo{
			Phone: req.GetDetails().GetContact().GetPhone(),
			Email: req.GetProductReview().GetCounterparty().GetEmail(),
		},
	}
	calculateReviewModerationReply, err := s.reviewModerationCalculationClient.CalculateReviewModeration(ctx, calculateReviewModerationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationCalculationService/CalculateReviewModeration")
	}
	step := &productReviewValidationServerValidateProductReviewCheckDocumentEligibilityStep{request: &documentsv1.CheckDocumentEligibilityRequest{
		Context: req.GetContext(),
		Document: &documentsv1.Document{
			Settlement: &documentsv1.Document_Card{Card: &documentsv1.DocumentCardSettlement{Token: req.GetProductReview().GetCard().GetToken()}},
		},
	}}
	if err := s.runValidateProductReviewCheckDocumentEligibility(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentValidationService/CheckDocumentEligibility")
	}
	checkDocumentEligibilityReply := step.reply
	return &reviewsv1.ValidateProductReviewResponse{
		ProductReview: &reviewsv1.ProductReview{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: calculateReviewModerationReply.GetReviewModeration().GetAudit().GetUpdatedBy(),
			},
			Counterparty: &customersv1.CustomerRef{
				Email: calculateReviewModerationReply.GetReviewModeration().GetCustomer().GetEmail(),
			},
			ProductReviewId: req.GetProductReviewId(),
			Status:          req.GetProductReview().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: checkDocumentEligibilityReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// PreviewProductReview previews a product review.
func (s *ProductReviewValidationServer) PreviewProductReview(ctx context.Context, req *reviewsv1.PreviewProductReviewRequest) (*reviewsv1.PreviewProductReviewResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PreviewProductReview", "error", err)
		return nil, err
	}
	if err := s.store.SaveProductReview(ctx, req.GetContext().GetTenantId(), req.GetProductReview()); err != nil {
		return nil, grpcx.Wrap(err, "save product review")
	}
	s.log.InfoContext(ctx, "preview product review", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductReview().GetAudit().GetEtag())
	step := &productReviewValidationServerPreviewProductReviewCalculateReviewModerationStep{request: &reviewsv1.CalculateReviewModerationRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.CalculateReviewModerationRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: req.GetProductReview().GetBillingAddress().GetCountryCode(),
			},
		},
		ReviewModeration: &reviewsv1.ReviewModeration{
			Requester: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
			Comments: []*reviewsv1.ReviewModerationComment{&reviewsv1.ReviewModerationComment{CreatedAt: req.GetProductReview().GetAudit().GetCreatedAt()}},
		},
	}}
	if err := s.runPreviewProductReviewCalculateReviewModeration(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationCalculationService/CalculateReviewModeration")
	}
	calculateReviewModerationReply := step.reply
	explainReviewModerationCalculationReply, err := s.reviewModerationCalculationClient.ExplainReviewModerationCalculation(ctx, &reviewsv1.ExplainReviewModerationCalculationRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Origin: &reviewsv1.ReviewModeration_Customer{Customer: &customersv1.CustomerRef{
				CustomerId: req.GetProductReview().GetCounterparty().GetCustomerId(),
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationCalculationService/ExplainReviewModerationCalculation")
	}
	return &reviewsv1.PreviewProductReviewResponse{
		ProductReview: &reviewsv1.ProductReview{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: calculateReviewModerationReply.GetReviewModeration().GetAudit().GetUpdatedBy(),
			},
			Counterparty: &customersv1.CustomerRef{
				Email: explainReviewModerationCalculationReply.GetReviewModeration().GetAssignee().GetEmail(),
			},
			ProductReviewId: req.GetProductReviewId(),
			Total:           req.GetProductReview().GetTotal(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckProductReviewEligibility checks a product review.
func (s *ProductReviewValidationServer) CheckProductReviewEligibility(ctx context.Context, req *reviewsv1.CheckProductReviewEligibilityRequest) (*reviewsv1.CheckProductReviewEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveProductReview(ctx, req.GetContext().GetTenantId(), req.GetProductReview()); err != nil {
		return nil, grpcx.Wrap(err, "save product review")
	}
	s.log.InfoContext(ctx, "check product review eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductReview().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "postal_code", req.GetProductReview().GetBillingAddress().GetPostalCode())
	recalculatePipelinesRequest := &pipelinesv1.RecalculatePipelinesRequest{
		Context: req.GetContext(),
		Pipeline: &pipelinesv1.Pipeline{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetProductReview().GetAudit().GetCreatedAt()},
			Participants: []*pipelinesv1.PipelineParticipant{&pipelinesv1.PipelineParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
			}},
		},
		Details: &pipelinesv1.RecalculatePipelinesRequest_Details{
			Destination: &commonv1.PostalAddress{PostalCode: tenancy.Value(ctx, "postal_code")},
		},
	}
	recalculatePipelinesReply, err := s.meteredPipelineCalculation.RecalculatePipelines(ctx, recalculatePipelinesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pipelines.v1.PipelineCalculationService/RecalculatePipelines")
	}
	detailsCache := cache.New[string, *reviewsv1.CheckProductReviewEligibilityRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	calculateReviewModerationRequest := &reviewsv1.CalculateReviewModerationRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: recalculatePipelinesReply.GetPipeline().GetAudit().GetUpdatedBy(),
			},
			Origin: &reviewsv1.ReviewModeration_Customer{Customer: &customersv1.CustomerRef{Email: cachedDetails.GetContact().GetEmail()}},
		},
		Details: &reviewsv1.CalculateReviewModerationRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: recalculatePipelinesReply.GetPipeline().GetLocation().GetCountryCode(),
			},
		},
	}
	send := s.calculateReviewModerationSender(calculateReviewModerationRequest)
	calculateReviewModerationReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationCalculationService/CalculateReviewModeration")
	}
	return &reviewsv1.CheckProductReviewEligibilityResponse{
		ProductReview: &reviewsv1.ProductReview{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: recalculatePipelinesReply.GetPipeline().GetLocation().GetCountryCode(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: calculateReviewModerationReply.GetReviewModeration().GetAudit().GetCreatedBy(),
			},
			ProductReviewId: req.GetProductReviewId(),
			DueAt:           req.GetProductReview().GetDueAt(),
		},
		Result: &commonv1.OperationResult{
			Details: recalculatePipelinesReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &reviewsv1.CheckProductReviewEligibilityResponse_Outcome{
			Status: calculateReviewModerationReply.GetResult().GetStatus(),
		},
	}, nil
}

// ExplainProductReviewDecision explains a product review.
func (s *ProductReviewValidationServer) ExplainProductReviewDecision(ctx context.Context, req *reviewsv1.ExplainProductReviewDecisionRequest) (*reviewsv1.ExplainProductReviewDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveProductReview(ctx, req.GetContext().GetTenantId(), req.GetProductReview()); err != nil {
		return nil, grpcx.Wrap(err, "save product review")
	}
	s.log.InfoContext(ctx, "explain product review decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductReview().GetAudit().GetEtag())
	listNotificationsReply, err := s.notificationClient.ListNotifications(ctx, &notificationsv1.ListNotificationsRequest{
		Context: req.GetContext(),
		Filter: &notificationsv1.ListNotificationsRequest_Filter{
			Region: req.GetProductReview().GetBillingAddress().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationService/ListNotifications")
	}
	recalculateReviewModerationsRequest := &reviewsv1.RecalculateReviewModerationsRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.RecalculateReviewModerationsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region: req.GetProductReview().GetBillingAddress().GetRegion(),
			},
		},
		ReviewModeration: &reviewsv1.ReviewModeration{
			Requester: &commonv1.ContactInfo{Phone: req.GetDetails().GetContact().GetPhone()},
		},
	}
	recalculateReviewModerationsReply, err := reviewsv1.ReviewModerationCalculationServiceClient.RecalculateReviewModerations(s.reviewModerationCalculationClient, ctx, recalculateReviewModerationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationCalculationService/RecalculateReviewModerations")
	}
	return &reviewsv1.ExplainProductReviewDecisionResponse{
		Result: &commonv1.OperationResult{
			Message: listNotificationsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &reviewsv1.ExplainProductReviewDecisionResponse_Outcome{Message: listNotificationsReply.GetResult().GetMessage()},
		ProductReview: &reviewsv1.ProductReview{
			DueAt:           recalculateReviewModerationsReply.GetReviewModeration().GetSla().GetDueAt(),
			ProductReviewId: req.GetProductReviewId(),
			Notes:           req.GetProductReview().GetNotes(),
		},
	}, nil
}

// ScoreProductReview scores a product review.
func (s *ProductReviewValidationServer) ScoreProductReview(ctx context.Context, req *reviewsv1.ScoreProductReviewRequest) (*reviewsv1.ScoreProductReviewResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveProductReview(ctx, req.GetContext().GetTenantId(), req.GetProductReview()); err != nil {
		return nil, grpcx.Wrap(err, "save product review")
	}
	s.log.InfoContext(ctx, "score product review", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProductReview().GetAudit().GetEtag())
	source := req.GetProductReview().GetAudit().ProtoReflect()
	createdBy := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("created_by"))).String()
	lintNotificationsRequest := &notificationsv1.LintNotificationsRequest{
		Context: req.GetContext(),
		Notification: &notificationsv1.Notification{
			Audit: &commonv1.AuditInfo{
				CreatedBy: createdBy,
				UpdatedBy: req.GetProductReview().GetAudit().GetUpdatedBy(),
			},
			Comments: []*notificationsv1.NotificationComment{&notificationsv1.NotificationComment{CreatedAt: req.GetProductReview().GetAudit().GetCreatedAt()}},
		},
		Details: &notificationsv1.LintNotificationsRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetProductReview().GetCounterparty().GetEmail()},
		},
	}
	lintNotificationsReply, err := s.lintNotificationsFn(ctx, lintNotificationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationValidationService/LintNotifications")
	}
	ctx = tenancy.With(ctx, "postal_code", req.GetDetails().GetDestination().GetPostalCode())
	cloned := proto.Clone(req.GetProductReview()).(*reviewsv1.ProductReview)
	quoteReviewModerationRequest := &reviewsv1.QuoteReviewModerationRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Origin: &reviewsv1.ReviewModeration_Customer{Customer: &customersv1.CustomerRef{Email: req.GetProductReview().GetCounterparty().GetEmail()}},
			Audit:  &commonv1.AuditInfo{CreatedBy: req.GetProductReview().GetAudit().GetCreatedBy()},
		},
		Details: &reviewsv1.QuoteReviewModerationRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: tenancy.Value(ctx, "postal_code"),
				Recipient:  cloned.GetBillingAddress().GetRecipient(),
			},
		},
	}
	quoteReviewModerationReply, err := grpcx.NewClient(s.conn, reviewsv1.NewReviewModerationCalculationServiceClient).QuoteReviewModeration(ctx, quoteReviewModerationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationCalculationService/QuoteReviewModeration")
	}
	calculateSellerListingRequest := &sellersv1.CalculateSellerListingRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{
				Email:       quoteReviewModerationReply.GetReviewModeration().GetCustomer().GetEmail(),
				DisplayName: lintNotificationsReply.GetNotification().GetAssignee().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: lintNotificationsReply.GetNotification().GetAudit().GetCreatedBy(),
			},
		},
	}
	calculateSellerListingReply, err := grpcx.NewClient(s.conn, sellersv1.NewSellerListingCalculationServiceClient).CalculateSellerListing(ctx, calculateSellerListingRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingCalculationService/CalculateSellerListing")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := productReviewValidationServerRecalculateReviewModerationsSteps[mode]
	recalculateReviewModerationsRequest := &reviewsv1.RecalculateReviewModerationsRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: calculateSellerListingReply.GetSellerListing().GetAudit().GetUpdatedAt(),
			},
			Sla: &reviewsv1.ReviewModerationSla{
				PausedFor: lintNotificationsReply.GetNotification().GetSla().GetPausedFor(),
			},
		},
	}
	recalculateReviewModerationsReply, err := run(s, ctx, recalculateReviewModerationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationCalculationService/RecalculateReviewModerations")
	}
	return &reviewsv1.ScoreProductReviewResponse{
		Result: &commonv1.OperationResult{
			Details: lintNotificationsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		ProductReview: &reviewsv1.ProductReview{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: quoteReviewModerationReply.GetReviewModeration().GetAudit().GetUpdatedBy(),
			},
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: calculateSellerListingReply.GetSellerListing().GetAddress().GetCountryCode(),
			},
			DueAt:           recalculateReviewModerationsReply.GetReviewModeration().GetSla().GetDueAt(),
			ProductReviewId: req.GetProductReviewId(),
			Number:          req.GetProductReview().GetNumber(),
		},
		Breakdown: &reviewsv1.ScoreProductReviewResponse_Breakdown{Total: quoteReviewModerationReply.GetBreakdown().GetTotal()},
	}, nil
}

// GetProductReviewRules returns a product review.
func (s *ProductReviewValidationServer) GetProductReviewRules(ctx context.Context, req *reviewsv1.GetProductReviewRulesRequest) (*reviewsv1.GetProductReviewRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetProductReviewRules", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product_review", req.GetProductReviewId(), "get_product_review_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get product review rules", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *reviewsv1.EstimateReviewModerationRequest, ...grpc.CallOption) (*reviewsv1.EstimateReviewModerationResponse, error){
		"primary":  s.reviewModerationCalculationClient.EstimateReviewModeration,
		"fallback": s.reviewModerationCalculationClientFallback.EstimateReviewModeration,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	estimateReviewModerationRequest := &reviewsv1.EstimateReviewModerationRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Requester: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	estimateReviewModerationReply, err := route(ctx, estimateReviewModerationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationCalculationService/EstimateReviewModeration")
	}
	return &reviewsv1.GetProductReviewRulesResponse{
		Result: &commonv1.OperationResult{
			Message: estimateReviewModerationReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		ProductReview: &reviewsv1.ProductReview{
			Audit: &commonv1.AuditInfo{
				Etag: estimateReviewModerationReply.GetReviewModeration().GetAudit().GetEtag(),
			},
			ProductReviewId: req.GetProductReviewId(),
		},
	}, nil
}

// productReviewValidationServerValidateProductReviewCheckDocumentEligibilityStep carries the request of one step of
// ValidateProductReview and, once it ran, its reply.
type productReviewValidationServerValidateProductReviewCheckDocumentEligibilityStep struct {
	request *documentsv1.CheckDocumentEligibilityRequest
	reply   *documentsv1.CheckDocumentEligibilityResponse
}

func (s *ProductReviewValidationServer) runValidateProductReviewCheckDocumentEligibility(ctx context.Context, step *productReviewValidationServerValidateProductReviewCheckDocumentEligibilityStep) error {
	reply, err := s.documentValidationClient.CheckDocumentEligibility(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// productReviewValidationServerPreviewProductReviewCalculateReviewModerationStep carries the request of one step of
// PreviewProductReview and, once it ran, its reply.
type productReviewValidationServerPreviewProductReviewCalculateReviewModerationStep struct {
	request *reviewsv1.CalculateReviewModerationRequest
	reply   *reviewsv1.CalculateReviewModerationResponse
}

func (s *ProductReviewValidationServer) runPreviewProductReviewCalculateReviewModeration(ctx context.Context, step *productReviewValidationServerPreviewProductReviewCalculateReviewModerationStep) error {
	reply, err := s.reviewModerationCalculationClient.CalculateReviewModeration(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// calculateReviewModerationSender returns a function that sends one request to
// acme.reviews.v1.ReviewModerationCalculationService/CalculateReviewModeration.
func (s *ProductReviewValidationServer) calculateReviewModerationSender(request *reviewsv1.CalculateReviewModerationRequest) func(context.Context) (*reviewsv1.CalculateReviewModerationResponse, error) {
	return func(ctx context.Context) (*reviewsv1.CalculateReviewModerationResponse, error) {
		return s.reviewModerationCalculationClient.CalculateReviewModeration(ctx, request)
	}
}

// productReviewValidationServerRecalculateReviewModerationsSteps are the ways to run
// acme.reviews.v1.ReviewModerationCalculationService/RecalculateReviewModerations, by name.
var productReviewValidationServerRecalculateReviewModerationsSteps = map[string]func(*ProductReviewValidationServer, context.Context, *reviewsv1.RecalculateReviewModerationsRequest) (*reviewsv1.RecalculateReviewModerationsResponse, error){
	"default": (*ProductReviewValidationServer).recalculateReviewModerationsStep,
	"audited": (*ProductReviewValidationServer).recalculateReviewModerationsAuditedStep,
}

func (s *ProductReviewValidationServer) recalculateReviewModerationsStep(ctx context.Context, request *reviewsv1.RecalculateReviewModerationsRequest) (*reviewsv1.RecalculateReviewModerationsResponse, error) {
	return s.reviewModerationCalculationClient.RecalculateReviewModerations(ctx, request)
}

func (s *ProductReviewValidationServer) recalculateReviewModerationsAuditedStep(ctx context.Context, request *reviewsv1.RecalculateReviewModerationsRequest) (*reviewsv1.RecalculateReviewModerationsResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.reviews.v1.ReviewModerationCalculationService/RecalculateReviewModerations")
	return s.reviewModerationCalculationClient.RecalculateReviewModerations(ctx, request)
}
