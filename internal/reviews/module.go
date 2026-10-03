package reviews

import (
	"context"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	sponsorshipsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sponsorships/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"google.golang.org/grpc"
)

// Register registers every service of the reviews domain on the server.
func Register(server *grpc.Server, deps platform.Deps) {
	wireCalls(deps)
	deps.Clients.Put("acme.customers.v1.CustomerSegmentValidationService", customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.giftcards.v1.GiftCardValidationService", giftcardsv1.NewGiftCardValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.identity.v1.TenantExportService", identityv1.NewTenantExportServiceClient(deps.Conn))
	deps.Clients.Put("acme.reviews.v1.ReviewModerationApprovalService", reviewsv1.NewReviewModerationApprovalServiceClient(deps.Conn))
	deps.Clients.Put("acme.reviews.v1.ReviewModerationValidationService", reviewsv1.NewReviewModerationValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.reviews.v1.ReviewVoteCalculationService", reviewsv1.NewReviewVoteCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.sellers.v1.SellerPayoutScheduleValidationService", sellersv1.NewSellerPayoutScheduleValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.sellers.v1.SellerService", sellersv1.NewSellerServiceClient(deps.Conn))
	productReviewServer := NewProductReviewServer(deps)
	server.RegisterService(&reviewsv1.ProductReviewService_ServiceDesc, productReviewServer)
	reviewsv1.RegisterReviewModerationQueryServiceServer(server, NewReviewModerationQueryServer(deps))
	reviewsv1.RegisterReviewVoteWorkflowServiceServer(server, NewReviewVoteWorkflowServer(deps))
	reviewsv1.RegisterProductReviewQueryServiceServer(server, NewProductReviewQueryServer(deps))
	reviewsv1.RegisterReviewModerationWorkflowServiceServer(server, NewReviewModerationWorkflowServer(deps))
	reviewsv1.RegisterReviewVoteApprovalServiceServer(server, NewReviewVoteApprovalServer(deps))
	productReviewWorkflowServer := NewProductReviewWorkflowServer(deps)
	server.RegisterService(&reviewsv1.ProductReviewWorkflowService_ServiceDesc, productReviewWorkflowServer)
	reviewsv1.RegisterReviewModerationApprovalServiceServer(server, NewReviewModerationApprovalServer(deps))
	reviewVoteValidationServer := NewReviewVoteValidationServer(deps)
	server.RegisterService(&reviewsv1.ReviewVoteValidationService_ServiceDesc, reviewVoteValidationServer)
	reviewsv1.RegisterProductReviewApprovalServiceServer(server, NewProductReviewApprovalServer(deps))
	reviewModerationValidationServer := NewReviewModerationValidationServer(deps)
	server.RegisterService(&reviewsv1.ReviewModerationValidationService_ServiceDesc, reviewModerationValidationServer)
	reviewsv1.RegisterReviewVoteCalculationServiceServer(server, NewReviewVoteCalculationServer(deps))
	productReviewValidationServer := NewProductReviewValidationServer(deps)
	server.RegisterService(&reviewsv1.ProductReviewValidationService_ServiceDesc, productReviewValidationServer)
	reviewModerationCalculationServer := NewReviewModerationCalculationServer(deps)
	server.RegisterService(&reviewsv1.ReviewModerationCalculationService_ServiceDesc, reviewModerationCalculationServer)
}

// The functions that call other services, which wireCalls sets when the module registers.
var (
	updateSellerCall                     func(context.Context, *sellersv1.UpdateSellerRequest, ...grpc.CallOption) (*sellersv1.UpdateSellerResponse, error)
	scoreSessionCall                     func(context.Context, *identityv1.ScoreSessionRequest) (*identityv1.ScoreSessionResponse, error)
	signalProductReviewWorkflowCall      func(context.Context, *reviewsv1.SignalProductReviewWorkflowRequest, ...grpc.CallOption) (*reviewsv1.SignalProductReviewWorkflowResponse, error)
	lintAuditEventsCall                  func(context.Context, *auditv1.LintAuditEventsRequest) (*auditv1.LintAuditEventsResponse, error)
	recalculateSellerPayoutSchedulesCall func(context.Context, *sellersv1.RecalculateSellerPayoutSchedulesRequest, ...grpc.CallOption) (*sellersv1.RecalculateSellerPayoutSchedulesResponse, error)
	scoreFeatureFlagCall                 func(context.Context, *featureflagsv1.ScoreFeatureFlagRequest, ...grpc.CallOption) (*featureflagsv1.ScoreFeatureFlagResponse, error)
	calculateReviewVoteCall              func(context.Context, *reviewsv1.CalculateReviewVoteRequest) (*reviewsv1.CalculateReviewVoteResponse, error)
)

// wireCalls points the package's call functions at the services they reach.
func wireCalls(deps platform.Deps) {
	sellerForUpdateSeller := sellersv1.NewSellerServiceClient(deps.Conn)
	updateSellerCall = sellerForUpdateSeller.UpdateSeller
	sessionValidationForScoreSession := identityv1.NewSessionValidationServiceClient(deps.Conn)
	scoreSessionCall = func(ctx context.Context, request *identityv1.ScoreSessionRequest) (*identityv1.ScoreSessionResponse, error) {
		return sessionValidationForScoreSession.ScoreSession(ctx, request)
	}
	productReviewWorkflowForSignalProductReviewWorkflow := reviewsv1.NewProductReviewWorkflowServiceClient(deps.Conn)
	signalProductReviewWorkflowCall = productReviewWorkflowForSignalProductReviewWorkflow.SignalProductReviewWorkflow
	auditEventValidationForLintAuditEvents := auditv1.NewAuditEventValidationServiceClient(deps.Conn)
	lintAuditEventsCall = func(ctx context.Context, request *auditv1.LintAuditEventsRequest) (*auditv1.LintAuditEventsResponse, error) {
		return auditEventValidationForLintAuditEvents.LintAuditEvents(ctx, request)
	}
	sellerPayoutScheduleCalculationForRecalculateSellerPayoutSchedules := sellersv1.NewSellerPayoutScheduleCalculationServiceClient(deps.Conn)
	recalculateSellerPayoutSchedulesCall = sellerPayoutScheduleCalculationForRecalculateSellerPayoutSchedules.RecalculateSellerPayoutSchedules
	featureFlagValidationForScoreFeatureFlag := featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn)
	scoreFeatureFlagCall = featureFlagValidationForScoreFeatureFlag.ScoreFeatureFlag
	reviewVoteCalculationForCalculateReviewVote := reviewsv1.NewReviewVoteCalculationServiceClient(deps.Conn)
	calculateReviewVoteCall = func(ctx context.Context, request *reviewsv1.CalculateReviewVoteRequest) (*reviewsv1.CalculateReviewVoteResponse, error) {
		return reviewVoteCalculationForCalculateReviewVote.CalculateReviewVote(ctx, request)
	}
}

// workers make the calls the domain's events ask for, apart from the requests that published them.
type workers struct {
	log                *slog.Logger
	sponsorQueryClient sponsorshipsv1.SponsorQueryServiceClient
}

// StartWorkers subscribes the domain's workers to their topics until the context ends.
func StartWorkers(ctx context.Context, deps platform.Deps) {
	w := &workers{
		log:                deps.Logger,
		sponsorQueryClient: sponsorshipsv1.NewSponsorQueryServiceClient(deps.Conn),
	}
	go w.consume(ctx, "reviews.lookup_review_moderation.suggest_sponsors", deps.Bus.Subscribe("reviews.lookup_review_moderation.suggest_sponsors"), w.handleReviewsLookupReviewModerationSuggestSponsors)
}

func (w *workers) consume(ctx context.Context, topic string, events <-chan eventbus.Event, handle func(context.Context, eventbus.Event) error) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-events:
			if err := handle(ctx, event); err != nil {
				w.log.ErrorContext(ctx, "event failed", "topic", topic, "error", err)
			}
		}
	}
}

// handleReviewsLookupReviewModerationSuggestSponsors makes the call the
// reviews.lookup_review_moderation.suggest_sponsors event asks for.
func (w *workers) handleReviewsLookupReviewModerationSuggestSponsors(ctx context.Context, event eventbus.Event) error {
	_, err := w.sponsorQueryClient.SuggestSponsors(ctx, &sponsorshipsv1.SuggestSponsorsRequest{
		Filter: &sponsorshipsv1.SuggestSponsorsRequest_Filter{OwnerId: event.Attributes["filter.owner_id"]},
		Context: &commonv1.RequestContext{
			TenantId: event.Attributes["context.tenant_id"],
			Actor: &commonv1.Actor{
				UserId:         "monolith",
				ServiceAccount: true,
			},
		},
	})
	return err
}
