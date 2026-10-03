package giftcards

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	mediav1 "github.com/hounddogai/protomap-test-golang/gen/acme/media/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	payrollv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payroll/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
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

// GiftCardBatchQueryServer implements `acme.giftcards.v1.GiftCardBatchQueryService`.
// Answers read-only questions about gift card batches, for screens and reports.
type GiftCardBatchQueryServer struct {
	giftcardsv1.UnimplementedGiftCardBatchQueryServiceServer
	store                                *Store
	log                                  *slog.Logger
	mediaAssetQueryClient                mediav1.MediaAssetQueryServiceClient
	conn                                 *grpc.ClientConn
	giftCardBatchValidationOnce          sync.Once
	giftCardBatchValidationCached        giftcardsv1.GiftCardBatchValidationServiceClient
	giftCardValidationClient             giftcardsv1.GiftCardValidationServiceClient
	meteredGiftCardTransactionValidation meteredGiftCardTransactionValidation
	notificationQueryClient              notificationsv1.NotificationQueryServiceClient
	clients                              *registry.Registry
	payRunCalculationClient              payrollv1.PayRunCalculationServiceClient
	documentQueryClient                  documentsv1.DocumentQueryServiceClient
	giftCardQueryClient                  giftcardsv1.GiftCardQueryServiceClient
	giftCardTransactionCalculationClient giftcardsv1.GiftCardTransactionCalculationServiceClient
}

// NewGiftCardBatchQueryServer wires a GiftCardBatchQueryServer.
func NewGiftCardBatchQueryServer(deps platform.Deps) *GiftCardBatchQueryServer {
	return &GiftCardBatchQueryServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.giftcards.v1.GiftCardBatchQueryService"),
		mediaAssetQueryClient:                mediav1.NewMediaAssetQueryServiceClient(deps.Conn),
		conn:                                 deps.Conn,
		giftCardValidationClient:             giftcardsv1.NewGiftCardValidationServiceClient(deps.Conn),
		meteredGiftCardTransactionValidation: meteredGiftCardTransactionValidation{GiftCardTransactionValidationServiceClient: giftcardsv1.NewGiftCardTransactionValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		notificationQueryClient:              notificationsv1.NewNotificationQueryServiceClient(deps.Conn),
		clients:                              deps.Clients,
		payRunCalculationClient:              payrollv1.NewPayRunCalculationServiceClient(deps.Conn),
		documentQueryClient:                  documentsv1.NewDocumentQueryServiceClient(deps.Conn),
		giftCardQueryClient:                  giftcardsv1.NewGiftCardQueryServiceClient(deps.Conn),
		giftCardTransactionCalculationClient: giftcardsv1.NewGiftCardTransactionCalculationServiceClient(deps.Conn),
	}
}

// LookupGiftCardBatch returns a gift card batch.
func (s *GiftCardBatchQueryServer) LookupGiftCardBatch(ctx context.Context, req *giftcardsv1.LookupGiftCardBatchRequest) (*giftcardsv1.LookupGiftCardBatchResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "gift_card_batch", req.GetGiftCardBatchId(), "lookup_gift_card_batch"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup gift card batch", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	countMediaAssetsRequest := &mediav1.CountMediaAssetsRequest{
		Context: req.GetContext(),
		Filter: &mediav1.CountMediaAssetsRequest_Filter{
			CreatedAfter: req.GetContext().GetDeadline(),
			OwnerId:      req.GetGiftCardBatchId(),
			Labels:       baggage,
		},
	}
	countMediaAssetsReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*mediav1.CountMediaAssetsResponse, error) {
		return s.mediaAssetQueryClient.CountMediaAssets(ctx, countMediaAssetsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.media.v1.MediaAssetQueryService/CountMediaAssets")
	}
	return &giftcardsv1.LookupGiftCardBatchResponse{
		Result:        &commonv1.OperationResult{Status: countMediaAssetsReply.GetResult().GetStatus()},
		GiftCardBatch: &giftcardsv1.GiftCardBatch{GiftCardBatchId: req.GetGiftCardBatchId()},
	}, nil
}

// BrowseGiftCardBatches lists gift card batches a page at a time.
func (s *GiftCardBatchQueryServer) BrowseGiftCardBatches(ctx context.Context, req *giftcardsv1.BrowseGiftCardBatchesRequest) (*giftcardsv1.BrowseGiftCardBatchesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected BrowseGiftCardBatches", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "browse gift card batches", "tenant", req.GetContext().GetTenantId())
	filterCache := cache.New[string, *giftcardsv1.BrowseGiftCardBatchesRequest_Filter](1)
	filterCache.Put(req.GetContext().GetTraceId(), req.GetFilter())
	cachedFilter, _ := filterCache.Get(req.GetContext().GetTraceId())
	explainGiftCardBatchDecisionRequest := &giftcardsv1.ExplainGiftCardBatchDecisionRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			BillingAddress: &commonv1.PostalAddress{Region: cachedFilter.GetRegion()},
		},
	}
	explainGiftCardBatchDecisionReply, err := s.giftCardBatchValidationLazy().ExplainGiftCardBatchDecision(ctx, explainGiftCardBatchDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchValidationService/ExplainGiftCardBatchDecision")
	}
	return &giftcardsv1.BrowseGiftCardBatchesResponse{
		GiftCardBatches: []*giftcardsv1.GiftCardBatch{&giftcardsv1.GiftCardBatch{
			BillingAddress: &commonv1.PostalAddress{
				Locality: explainGiftCardBatchDecisionReply.GetGiftCardBatch().GetBillingAddress().GetLocality(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// FindGiftCardBatches lists gift card batches a page at a time.
func (s *GiftCardBatchQueryServer) FindGiftCardBatches(ctx context.Context, req *giftcardsv1.FindGiftCardBatchesRequest) (*giftcardsv1.FindGiftCardBatchesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "find gift card batches", "tenant", req.GetContext().GetTenantId())
	return &giftcardsv1.FindGiftCardBatchesResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// CountGiftCardBatches counts gift card batches by status.
func (s *GiftCardBatchQueryServer) CountGiftCardBatches(ctx context.Context, req *giftcardsv1.CountGiftCardBatchesRequest) (*giftcardsv1.CountGiftCardBatchesResponse, error) {
	s.log.InfoContext(ctx, "count gift card batches", "tenant", req.GetContext().GetTenantId())
	scoreGiftCardRequest := &giftcardsv1.ScoreGiftCardRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.ScoreGiftCardRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	scoreGiftCardReply, err := giftcardsv1.GiftCardValidationServiceClient.ScoreGiftCard(s.giftCardValidationClient, ctx, scoreGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/ScoreGiftCard")
	}
	return &giftcardsv1.CountGiftCardBatchesResponse{
		Result: &commonv1.OperationResult{
			Message: scoreGiftCardReply.GetResult().GetMessage(),
			Details: scoreGiftCardReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// ResolveGiftCardBatches returns several gift card batches at once.
func (s *GiftCardBatchQueryServer) ResolveGiftCardBatches(ctx context.Context, req *giftcardsv1.ResolveGiftCardBatchesRequest) (*giftcardsv1.ResolveGiftCardBatchesResponse, error) {
	s.log.InfoContext(ctx, "resolve gift card batches", "tenant", req.GetContext().GetTenantId())
	var holder struct{ Locale string }
	if record := req.GetContext(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Locale"); err != nil {
			s.log.WarnContext(ctx, "copy locale", "error", err)
		}
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	checkGiftCardTransactionEligibilityRequest := &giftcardsv1.CheckGiftCardTransactionEligibilityRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.CheckGiftCardTransactionEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  holder.Locale,
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Measurements: []*giftcardsv1.GiftCardTransactionMeasurement{&giftcardsv1.GiftCardTransactionMeasurement{ObservedAt: req.GetContext().GetDeadline()}},
			Source:       req.GetContext().GetActor().GetUserId(),
			Detail:       packed,
		},
	}
	checkGiftCardTransactionEligibilityReply, err := s.meteredGiftCardTransactionValidation.CheckGiftCardTransactionEligibility(ctx, checkGiftCardTransactionEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionValidationService/CheckGiftCardTransactionEligibility")
	}
	exportNotificationsStream, err := s.notificationQueryClient.ExportNotifications(ctx, &notificationsv1.ExportNotificationsRequest{
		Context: req.GetContext(),
		Filter: &notificationsv1.ExportNotificationsRequest_Filter{
			CreatedAfter: checkGiftCardTransactionEligibilityReply.GetGiftCardTransaction().GetOccurredAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationQueryService/ExportNotifications")
	}
	for {
		received, err := exportNotificationsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationQueryService/ExportNotifications")
		}
		_ = received
	}
	return &giftcardsv1.ResolveGiftCardBatchesResponse{
		Result: &commonv1.OperationResult{
			Details: checkGiftCardTransactionEligibilityReply.GetResult().GetDetails(),
			Status:  checkGiftCardTransactionEligibilityReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// ExportGiftCardBatches streams every matching gift card batch.
func (s *GiftCardBatchQueryServer) ExportGiftCardBatches(req *giftcardsv1.ExportGiftCardBatchesRequest, stream grpc.ServerStreamingServer[giftcardsv1.ExportGiftCardBatchesResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "export gift card batches", "tenant", req.GetContext().GetTenantId())
	searchIndexNotification, err := registry.Lookup[searchv1.SearchIndexNotificationServiceClient](s.clients, "acme.search.v1.SearchIndexNotificationService")
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	packed, err := anypb.New(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "pack payload")
	}
	unsubscribeSearchIndexUpdatesRequest := &searchv1.UnsubscribeSearchIndexUpdatesRequest{
		Context: req.GetContext(),
		Details: &searchv1.UnsubscribeSearchIndexUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: cachedContext.GetActor().GetEmail()},
			Payload:   packed,
		},
	}
	unsubscribeSearchIndexUpdatesReply, err := searchIndexNotification.UnsubscribeSearchIndexUpdates(ctx, unsubscribeSearchIndexUpdatesRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/UnsubscribeSearchIndexUpdates")
	}
	calculatePayRunRequest := newCalculatePayRunRequest(withCalculatePayRunContext(req.GetContext()), withCalculatePayRunDetails(&payrollv1.CalculatePayRunRequest_Details{
		Destination: &commonv1.PostalAddress{
			Region: unsubscribeSearchIndexUpdatesReply.GetSearchIndex().GetAddress().GetRegion(),
			Lines:  unsubscribeSearchIndexUpdatesReply.GetSearchIndex().GetAddress().GetLines(),
		},
	}), withCalculatePayRunPayRun(&payrollv1.PayRun{
		Counterparty: &payrollv1.DeductionRef{
			DisplayName: unsubscribeSearchIndexUpdatesReply.GetSearchIndex().GetOwner().GetDisplayName(),
		},
		BillingAddress: &commonv1.PostalAddress{
			CountryCode: unsubscribeSearchIndexUpdatesReply.GetSearchIndex().GetAddress().GetCountryCode(),
		},
	}))
	calculatePayRunReply, err := s.payRunCalculationClient.CalculatePayRun(ctx, calculatePayRunRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.payroll.v1.PayRunCalculationService/CalculatePayRun")
	}
	if err := stream.Send(&giftcardsv1.ExportGiftCardBatchesResponse{
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: unsubscribeSearchIndexUpdatesReply.GetSearchIndex().GetAudit().GetUpdatedAt(),
			},
			Notes:          calculatePayRunReply.GetPayRun().GetNotes(),
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizeGiftCardBatches summarizes a gift card batch.
func (s *GiftCardBatchQueryServer) SummarizeGiftCardBatches(ctx context.Context, req *giftcardsv1.SummarizeGiftCardBatchesRequest) (*giftcardsv1.SummarizeGiftCardBatchesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "gift_card_batch", req.GetGiftCardBatchId(), "summarize_gift_card_batches"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize gift card batches", "tenant", req.GetContext().GetTenantId())
	suggestDocumentsRequest := newSuggestDocumentsRequest(withSuggestDocumentsContext(req.GetContext()), withSuggestDocumentsFilter(&documentsv1.SuggestDocumentsRequest_Filter{
		Labels:  req.GetAnnotations(),
		OwnerId: req.GetGiftCardBatchId(),
	}))
	suggestDocumentsReply, err := s.documentQueryClient.SuggestDocuments(ctx, suggestDocumentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentQueryService/SuggestDocuments")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	browseGiftCardsRequest := &giftcardsv1.BrowseGiftCardsRequest{
		Context: req.GetContext(),
		Filter: &giftcardsv1.BrowseGiftCardsRequest_Filter{
			OwnerId:      suggestDocumentsReply.GetResult().GetCorrelationId(),
			Labels:       annotations,
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	}
	browseGiftCardsReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*giftcardsv1.BrowseGiftCardsResponse, error) {
		return s.giftCardQueryClient.BrowseGiftCards(ctx, browseGiftCardsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardQueryService/BrowseGiftCards")
	}
	return &giftcardsv1.SummarizeGiftCardBatchesResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: suggestDocumentsReply.GetResult().GetReferenceIds(),
			Details:      browseGiftCardsReply.GetResult().GetDetails(),
			Status:       "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestGiftCardBatches lists gift card batches a page at a time.
func (s *GiftCardBatchQueryServer) SuggestGiftCardBatches(ctx context.Context, req *giftcardsv1.SuggestGiftCardBatchesRequest) (*giftcardsv1.SuggestGiftCardBatchesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuggestGiftCardBatches", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "suggest gift card batches", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "region", req.GetFilter().GetRegion())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	simulateGiftCardTransactionRequest := &giftcardsv1.SimulateGiftCardTransactionRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.SimulateGiftCardTransactionRequest_Details{
			Destination: &commonv1.PostalAddress{Region: tenancy.Value(ctx, "region")},
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{OccurredAt: req.GetFilter().GetCreatedAfter()},
		Annotations:         labels,
	}
	send := s.simulateGiftCardTransactionSender(simulateGiftCardTransactionRequest)
	simulateGiftCardTransactionReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/SimulateGiftCardTransaction")
	}
	return &giftcardsv1.SuggestGiftCardBatchesResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: simulateGiftCardTransactionReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// PeekGiftCardBatch returns a gift card batch.
func (s *GiftCardBatchQueryServer) PeekGiftCardBatch(ctx context.Context, req *giftcardsv1.PeekGiftCardBatchRequest) (*giftcardsv1.PeekGiftCardBatchResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "gift_card_batch", req.GetGiftCardBatchId(), "peek_gift_card_batch"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek gift card batch", "tenant", req.GetContext().GetTenantId())
	return &giftcardsv1.PeekGiftCardBatchResponse{
		GiftCardBatch: &giftcardsv1.GiftCardBatch{GiftCardBatchId: req.GetGiftCardBatchId()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// giftCardBatchValidationLazy dials acme.giftcards.v1.GiftCardBatchValidationService the first time a handler needs it.
func (s *GiftCardBatchQueryServer) giftCardBatchValidationLazy() giftcardsv1.GiftCardBatchValidationServiceClient {
	s.giftCardBatchValidationOnce.Do(func() { s.giftCardBatchValidationCached = giftcardsv1.NewGiftCardBatchValidationServiceClient(s.conn) })
	return s.giftCardBatchValidationCached
}

// simulateGiftCardTransactionSender returns a function that sends one request to
// acme.giftcards.v1.GiftCardTransactionCalculationService/SimulateGiftCardTransaction.
func (s *GiftCardBatchQueryServer) simulateGiftCardTransactionSender(request *giftcardsv1.SimulateGiftCardTransactionRequest) func(context.Context) (*giftcardsv1.SimulateGiftCardTransactionResponse, error) {
	return func(ctx context.Context) (*giftcardsv1.SimulateGiftCardTransactionResponse, error) {
		return s.giftCardTransactionCalculationClient.SimulateGiftCardTransaction(ctx, request)
	}
}
