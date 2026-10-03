package reviews

import (
	"context"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	payrollv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payroll/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// ReviewModerationCalculationServer implements `acme.reviews.v1.ReviewModerationCalculationService`.
// Computes the figures of a review moderation, such as totals, taxes, and estimates.
type ReviewModerationCalculationServer struct {
	reviewsv1.UnimplementedReviewModerationCalculationServiceServer
	store                                *Store
	log                                  *slog.Logger
	clients                              *registry.Registry
	conn                                 *grpc.ClientConn
	collectionClient                     merchandisingv1.CollectionServiceClient
	notificationClient                   notificationsv1.NotificationServiceClient
	giftCardTransactionCalculationClient giftcardsv1.GiftCardTransactionCalculationServiceClient
	apiCredentialValidationClient        identityv1.ApiCredentialValidationServiceClient
}

// NewReviewModerationCalculationServer wires a ReviewModerationCalculationServer.
func NewReviewModerationCalculationServer(deps platform.Deps) *ReviewModerationCalculationServer {
	return &ReviewModerationCalculationServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.reviews.v1.ReviewModerationCalculationService"),
		clients:                              deps.Clients,
		conn:                                 deps.Conn,
		collectionClient:                     merchandisingv1.NewCollectionServiceClient(deps.Conn),
		notificationClient:                   notificationsv1.NewNotificationServiceClient(deps.Conn),
		giftCardTransactionCalculationClient: giftcardsv1.NewGiftCardTransactionCalculationServiceClient(deps.Conn),
		apiCredentialValidationClient:        identityv1.NewApiCredentialValidationServiceClient(deps.Conn),
	}
}

// CalculateReviewModeration calculates a review moderation.
func (s *ReviewModerationCalculationServer) CalculateReviewModeration(ctx context.Context, req *reviewsv1.CalculateReviewModerationRequest) (*reviewsv1.CalculateReviewModerationResponse, error) {
	if err := s.store.SaveReviewModeration(ctx, req.GetContext().GetTenantId(), req.GetReviewModeration()); err != nil {
		return nil, grpcx.Wrap(err, "save review moderation")
	}
	s.log.InfoContext(ctx, "calculate review moderation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReviewModeration().GetAudit().GetEtag())
	seller, err := registry.Lookup[sellersv1.SellerServiceClient](s.clients, "acme.sellers.v1.SellerService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	createSellerRequest := &sellersv1.CreateSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			PrimaryAddress:      &commonv1.PostalAddress{Recipient: req.GetDetails().GetDestination().GetRecipient()},
			RelationshipManager: &identityv1.UserRef{Email: req.GetReviewModeration().GetCustomer().GetEmail()},
		},
	}
	createSellerReply, err := seller.CreateSeller(ctx, createSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerService/CreateSeller")
	}
	lockTenantRequest := &identityv1.LockTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Details: &identityv1.LockTenantRequest_Details{
			NotifyContact: &commonv1.ContactInfo{
				Name:  req.GetReviewModeration().GetRequester().GetName(),
				Phone: req.GetReviewModeration().GetRequester().GetPhone(),
			},
		},
	}
	lockTenantReply := new(identityv1.LockTenantResponse)
	if err := s.conn.Invoke(ctx, identityTenantAdminServiceLockTenantMethod, lockTenantRequest, lockTenantReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/LockTenant")
	}
	return &reviewsv1.CalculateReviewModerationResponse{
		ReviewModeration: &reviewsv1.ReviewModeration{
			Audit: &commonv1.AuditInfo{
				Etag:      createSellerReply.GetSeller().GetAudit().GetEtag(),
				UpdatedAt: lockTenantReply.GetTenant().GetAudit().GetUpdatedAt(),
			},
			ReviewModerationId: req.GetReviewModerationId(),
			Requester:          req.GetReviewModeration().GetRequester(),
		},
		Result: &commonv1.OperationResult{
			Message: lockTenantReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// EstimateReviewModeration estimates a review moderation.
func (s *ReviewModerationCalculationServer) EstimateReviewModeration(ctx context.Context, req *reviewsv1.EstimateReviewModerationRequest) (*reviewsv1.EstimateReviewModerationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReviewModeration(ctx, req.GetContext().GetTenantId(), req.GetReviewModeration()); err != nil {
		return nil, grpcx.Wrap(err, "save review moderation")
	}
	s.log.InfoContext(ctx, "estimate review moderation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReviewModeration().GetAudit().GetEtag())
	resolvePayRunsRequest := &payrollv1.ResolvePayRunsRequest{Context: req.GetContext()}
	resolvePayRunsReply := new(payrollv1.ResolvePayRunsResponse)
	if err := grpcx.Invoke(ctx, s.conn, payrollv1.PayRunQueryService_ResolvePayRuns_FullMethodName, resolvePayRunsRequest, resolvePayRunsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.payroll.v1.PayRunQueryService/ResolvePayRuns")
	}
	updateCollectionRequest := &merchandisingv1.UpdateCollectionRequest{
		Context: req.GetContext(),
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Location: &commonv1.PostalAddress{
						CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
					},
				}},
			}},
		},
	}
	updateCollectionReply, err := grpcx.Call(ctx, s.collectionClient.UpdateCollection, updateCollectionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionService/UpdateCollection")
	}
	return &reviewsv1.EstimateReviewModerationResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  resolvePayRunsReply.GetResult().GetReferenceIds(),
			CorrelationId: resolvePayRunsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		ReviewModeration: &reviewsv1.ReviewModeration{
			Assignee: &identityv1.UserRef{
				Email: updateCollectionReply.GetCollection().GetManager().GetEmail(),
			},
			ReviewModerationId: req.GetReviewModerationId(),
			CustomFields:       req.GetReviewModeration().GetCustomFields(),
		},
	}, nil
}

// QuoteReviewModeration quotes a review moderation.
func (s *ReviewModerationCalculationServer) QuoteReviewModeration(ctx context.Context, req *reviewsv1.QuoteReviewModerationRequest) (*reviewsv1.QuoteReviewModerationResponse, error) {
	if err := s.store.SaveReviewModeration(ctx, req.GetContext().GetTenantId(), req.GetReviewModeration()); err != nil {
		return nil, grpcx.Wrap(err, "save review moderation")
	}
	s.log.InfoContext(ctx, "quote review moderation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReviewModeration().GetAudit().GetEtag())
	estimateGiftCardRequest := &giftcardsv1.EstimateGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetReviewModeration().GetAudit().GetUpdatedAt(),
				Etag:      req.GetReviewModeration().GetAudit().GetEtag(),
			},
			Holder: &giftcardsv1.GiftCardTransactionRef{
				DisplayName: req.GetReviewModeration().GetCustomer().GetDisplayName(),
			},
			Contact: &commonv1.ContactInfo{Email: req.GetReviewModeration().GetRequester().GetEmail()},
		},
	}
	estimateGiftCardReply, err := grpcx.NewClient(s.conn, giftcardsv1.NewGiftCardCalculationServiceClient).EstimateGiftCard(ctx, estimateGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/EstimateGiftCard")
	}
	return &reviewsv1.QuoteReviewModerationResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: estimateGiftCardReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		ReviewModeration: &reviewsv1.ReviewModeration{
			ReviewModerationId: req.GetReviewModerationId(),
			Status:             req.GetReviewModeration().GetStatus(),
		},
	}, nil
}

// RecalculateReviewModerations recalculates a review moderation.
func (s *ReviewModerationCalculationServer) RecalculateReviewModerations(ctx context.Context, req *reviewsv1.RecalculateReviewModerationsRequest) (*reviewsv1.RecalculateReviewModerationsResponse, error) {
	if err := s.store.SaveReviewModeration(ctx, req.GetContext().GetTenantId(), req.GetReviewModeration()); err != nil {
		return nil, grpcx.Wrap(err, "save review moderation")
	}
	s.log.InfoContext(ctx, "recalculate review moderations", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReviewModeration().GetAudit().GetEtag())
	for _, comment := range req.GetReviewModeration().GetComments() {
		if _, err := s.notificationClient.CreateNotification(ctx, &notificationsv1.CreateNotificationRequest{
			Context: req.GetContext(),
			Notification: &notificationsv1.Notification{
				Audit:    &commonv1.AuditInfo{CreatedAt: comment.GetCreatedAt()},
				Comments: []*notificationsv1.NotificationComment{&notificationsv1.NotificationComment{AuthorEmail: comment.GetAuthorEmail()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationService/CreateNotification")
		}
	}
	giftCardValidation, ok := s.clients.Get("acme.giftcards.v1.GiftCardValidationService").(giftcardsv1.GiftCardValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.giftcards.v1.GiftCardValidationService")
	}
	scoreGiftCardRequest := &giftcardsv1.ScoreGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Contact: &commonv1.ContactInfo{
				Name:  req.GetReviewModeration().GetRequester().GetName(),
				Email: req.GetReviewModeration().GetAssignee().GetEmail(),
			},
		},
	}
	scoreGiftCardReply, err := giftCardValidation.ScoreGiftCard(ctx, scoreGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/ScoreGiftCard")
	}
	ctx = tenancy.With(ctx, "country_code", req.GetDetails().GetDestination().GetCountryCode())
	packed, err := anypb.New(req.GetReviewModeration())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	recalculateGiftCardTransactionsRequest := &giftcardsv1.RecalculateGiftCardTransactionsRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.RecalculateGiftCardTransactionsRequest_Details{
			Destination: &commonv1.PostalAddress{CountryCode: tenancy.Value(ctx, "country_code")},
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: req.GetReviewModeration().GetCustomer().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	recalculateGiftCardTransactionsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*giftcardsv1.RecalculateGiftCardTransactionsResponse, error) {
		return s.giftCardTransactionCalculationClient.RecalculateGiftCardTransactions(ctx, recalculateGiftCardTransactionsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/RecalculateGiftCardTransactions")
	}
	return &reviewsv1.RecalculateReviewModerationsResponse{
		ReviewModeration: &reviewsv1.ReviewModeration{
			Requester: &commonv1.ContactInfo{
				Name: scoreGiftCardReply.GetGiftCard().GetContact().GetName(),
			},
			Assignee: &identityv1.UserRef{
				DisplayName: recalculateGiftCardTransactionsReply.GetGiftCardTransaction().GetSubject().GetDisplayName(),
			},
			ReviewModerationId: req.GetReviewModerationId(),
			CustomFields:       req.GetReviewModeration().GetCustomFields(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: recalculateGiftCardTransactionsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// ExplainReviewModerationCalculation explains a review moderation.
func (s *ReviewModerationCalculationServer) ExplainReviewModerationCalculation(ctx context.Context, req *reviewsv1.ExplainReviewModerationCalculationRequest) (*reviewsv1.ExplainReviewModerationCalculationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainReviewModerationCalculation", "error", err)
		return nil, err
	}
	if err := s.store.SaveReviewModeration(ctx, req.GetContext().GetTenantId(), req.GetReviewModeration()); err != nil {
		return nil, grpcx.Wrap(err, "save review moderation")
	}
	s.log.InfoContext(ctx, "explain review moderation calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReviewModeration().GetAudit().GetEtag())
	jobs := make(chan *reviewsv1.ReviewModerationComment)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for comment := range jobs {
				if _, err := s.apiCredentialValidationClient.PreviewApiCredential(workCtx, &identityv1.PreviewApiCredentialRequest{
					Context: req.GetContext(),
					Details: &identityv1.PreviewApiCredentialRequest_Details{
						Contact: &commonv1.ContactInfo{Email: comment.GetAuthorEmail()},
					},
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
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialValidationService/PreviewApiCredential")
	}
	suggestMetricSeriesesRequest := &metricsv1.SuggestMetricSeriesesRequest{Context: req.GetContext()}
	suggestMetricSeriesesReply, err := grpcx.NewClient(s.conn, metricsv1.NewMetricSeriesQueryServiceClient).SuggestMetricSerieses(ctx, suggestMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/SuggestMetricSerieses")
	}
	return &reviewsv1.ExplainReviewModerationCalculationResponse{
		Outcome: &reviewsv1.ExplainReviewModerationCalculationResponse_Outcome{Status: suggestMetricSeriesesReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			ReferenceIds: suggestMetricSeriesesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		ReviewModeration: &reviewsv1.ReviewModeration{
			ReviewModerationId: req.GetReviewModerationId(),
			Requester:          req.GetReviewModeration().GetRequester(),
		},
	}, nil
}

// GetReviewModerationRates returns a review moderation.
func (s *ReviewModerationCalculationServer) GetReviewModerationRates(ctx context.Context, req *reviewsv1.GetReviewModerationRatesRequest) (*reviewsv1.GetReviewModerationRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetReviewModerationRates", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "review_moderation", req.GetReviewModerationId(), "get_review_moderation_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get review moderation rates", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	simulateGiftCardRequest := &giftcardsv1.SimulateGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Contact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
			Audit:   &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
			Funding: &giftcardsv1.GiftCard_ExternalAccountId{ExternalAccountId: req.GetReviewModerationId()},
		},
	}
	simulateGiftCardReply := new(giftcardsv1.SimulateGiftCardResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.giftcards.v1.GiftCardCalculationService/SimulateGiftCard", simulateGiftCardRequest, simulateGiftCardReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/SimulateGiftCard")
	}
	return &reviewsv1.GetReviewModerationRatesResponse{
		Result: &commonv1.OperationResult{Status: simulateGiftCardReply.GetResult().GetStatus()},
		ReviewModeration: &reviewsv1.ReviewModeration{
			Requester: &commonv1.ContactInfo{
				Name: simulateGiftCardReply.GetGiftCard().GetContact().GetName(),
			},
			ReviewModerationId: req.GetReviewModerationId(),
		},
	}, nil
}
