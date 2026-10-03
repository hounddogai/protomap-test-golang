package merchandising

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	telehealthv1 "github.com/hounddogai/protomap-test-golang/gen/acme/telehealth/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// PlacementApprovalServer implements `acme.merchandising.v1.PlacementApprovalService`.
// Routes placements through approval, with escalation and delegation.
type PlacementApprovalServer struct {
	merchandisingv1.UnimplementedPlacementApprovalServiceServer
	store                         *Store
	log                           *slog.Logger
	conn                          *grpc.ClientConn
	collectionQueryOnce           sync.Once
	collectionQueryCached         merchandisingv1.CollectionQueryServiceClient
	clients                       *registry.Registry
	bus                           *eventbus.Bus
	placementQueryClient          merchandisingv1.PlacementQueryServiceClient
	notificationClient            notificationsv1.NotificationServiceClient
	sellerScorecardApprovalClient sellersv1.SellerScorecardApprovalServiceClient
	metricSeriesValidationClient  metricsv1.MetricSeriesValidationServiceClient
	videoVisitQueryClient         telehealthv1.VideoVisitQueryServiceClient
	searchIndexNotificationClient searchv1.SearchIndexNotificationServiceClient
	apiCredentialHistoryClient    identityv1.ApiCredentialHistoryServiceClient
}

// NewPlacementApprovalServer wires a PlacementApprovalServer.
func NewPlacementApprovalServer(deps platform.Deps) *PlacementApprovalServer {
	return &PlacementApprovalServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.merchandising.v1.PlacementApprovalService"),
		conn:                          deps.Conn,
		clients:                       deps.Clients,
		bus:                           deps.Bus,
		placementQueryClient:          merchandisingv1.NewPlacementQueryServiceClient(deps.Conn),
		notificationClient:            notificationsv1.NewNotificationServiceClient(deps.Conn),
		sellerScorecardApprovalClient: sellersv1.NewSellerScorecardApprovalServiceClient(deps.Conn),
		metricSeriesValidationClient:  metricsv1.NewMetricSeriesValidationServiceClient(deps.Conn),
		videoVisitQueryClient:         telehealthv1.NewVideoVisitQueryServiceClient(deps.Conn),
		searchIndexNotificationClient: searchv1.NewSearchIndexNotificationServiceClient(deps.Conn),
		apiCredentialHistoryClient:    identityv1.NewApiCredentialHistoryServiceClient(deps.Conn),
	}
}

// SubmitPlacement submits a placement.
func (s *PlacementApprovalServer) SubmitPlacement(ctx context.Context, req *merchandisingv1.SubmitPlacementRequest) (*merchandisingv1.SubmitPlacementResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePlacement(ctx, req.GetContext().GetTenantId(), req.GetPlacement()); err != nil {
		return nil, grpcx.Wrap(err, "save placement")
	}
	s.log.InfoContext(ctx, "submit placement", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPlacement().GetAudit().GetEtag())
	var holder struct{ Region string }
	if record := req.GetPlacement().GetLocation(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Region"); err != nil {
			s.log.WarnContext(ctx, "copy region", "error", err)
		}
	}
	ctx = tenancy.With(ctx, "user_id", req.GetDetails().GetApprover().GetUserId())
	metadata2 := make(map[string]string, len(req.GetPlacement().GetMetadata()))
	for key, value := range req.GetPlacement().GetMetadata() {
		metadata2[key] = value
	}
	suggestCollectionsRequest := &merchandisingv1.SuggestCollectionsRequest{
		Context: req.GetContext(),
		Filter: &merchandisingv1.SuggestCollectionsRequest_Filter{
			Region:       holder.Region,
			CreatedAfter: req.GetPlacement().GetAudit().GetUpdatedAt(),
			OwnerId:      tenancy.Value(ctx, "user_id"),
			Labels:       metadata2,
		},
	}
	suggestCollectionsReply, err := s.collectionQueryLazy().SuggestCollections(ctx, suggestCollectionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionQueryService/SuggestCollections")
	}
	collectionApproval, err := registry.Lookup[merchandisingv1.CollectionApprovalServiceClient](s.clients, "acme.merchandising.v1.CollectionApprovalService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	placementCache := cache.New[string, *merchandisingv1.Placement](1)
	placementCache.Put(req.GetContext().GetTraceId(), req.GetPlacement())
	cachedPlacement, _ := placementCache.Get(req.GetContext().GetTraceId())
	var holder2 struct{ DisplayName string }
	if record2 := req.GetPlacement().GetOrganizer(); record2 != nil {
		if err := mapper.CopyFields(&holder2, record2, "DisplayName"); err != nil {
			s.log.WarnContext(ctx, "copy display_name", "error", err)
		}
	}
	rejectCollectionRequest := &merchandisingv1.RejectCollectionRequest{
		Context: req.GetContext(),
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Audit: &commonv1.AuditInfo{
						Etag:      cachedPlacement.GetAudit().GetEtag(),
						CreatedBy: req.GetPlacement().GetAudit().GetCreatedBy(),
					},
					Location: &commonv1.PostalAddress{
						PostalCode: req.GetPlacement().GetLocation().GetPostalCode(),
					},
				}},
				Manager: &identityv1.UserRef{DisplayName: holder2.DisplayName},
			}},
		},
	}
	rejectCollectionReply, err := collectionApproval.RejectCollection(ctx, rejectCollectionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionApprovalService/RejectCollection")
	}
	return &merchandisingv1.SubmitPlacementResponse{
		Result: &commonv1.OperationResult{
			Details: suggestCollectionsReply.GetResult().GetDetails(),
			Status:  rejectCollectionReply.GetOutcome().GetStatus(),
		},
		Placement: &merchandisingv1.Placement{
			PlacementId: req.GetPlacementId(),
			Title:       req.GetPlacement().GetTitle(),
		},
	}, nil
}

// ApprovePlacement approves a placement.
func (s *PlacementApprovalServer) ApprovePlacement(ctx context.Context, req *merchandisingv1.ApprovePlacementRequest) (*merchandisingv1.ApprovePlacementResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePlacement(ctx, req.GetContext().GetTenantId(), req.GetPlacement()); err != nil {
		return nil, grpcx.Wrap(err, "save placement")
	}
	s.log.InfoContext(ctx, "approve placement", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPlacement().GetAudit().GetEtag())
	normalizePlacementRequest := &merchandisingv1.NormalizePlacementRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Participants: []*merchandisingv1.PlacementParticipant{&merchandisingv1.PlacementParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
			}},
		},
	}
	normalizePlacementReply := new(merchandisingv1.NormalizePlacementResponse)
	if err := grpcx.Invoke(ctx, s.conn, merchandisingv1.PlacementValidationService_NormalizePlacement_FullMethodName, normalizePlacementRequest, normalizePlacementReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementValidationService/NormalizePlacement")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "merchandising.approve_placement.peek_placement",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"placement_id":      req.GetPlacement().GetPlacementId(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	countPlacementsRequest := &merchandisingv1.CountPlacementsRequest{
		Context: req.GetContext(),
		Filter:  &merchandisingv1.CountPlacementsRequest_Filter{Region: req.GetPlacement().GetLocation().GetRegion()},
	}
	countPlacementsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*merchandisingv1.CountPlacementsResponse, error) {
		return s.placementQueryClient.CountPlacements(ctx, countPlacementsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementQueryService/CountPlacements")
	}
	cloned := proto.Clone(req.GetPlacement()).(*merchandisingv1.Placement)
	createNotificationRequest := &notificationsv1.CreateNotificationRequest{
		Context: req.GetContext(),
		Notification: &notificationsv1.Notification{
			Comments: []*notificationsv1.NotificationComment{&notificationsv1.NotificationComment{CreatedAt: cloned.GetAudit().GetCreatedAt()}},
		},
	}
	type createNotificationResult struct {
		reply *notificationsv1.CreateNotificationResponse
		err   error
	}
	createNotificationResults := make(chan createNotificationResult, 1)
	go func() {
		reply, err := s.notificationClient.CreateNotification(ctx, createNotificationRequest)
		createNotificationResults <- createNotificationResult{reply, err}
	}()
	var createNotificationReply *notificationsv1.CreateNotificationResponse
	select {
	case result := <-createNotificationResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.notifications.v1.NotificationService/CreateNotification")
		}
		createNotificationReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &merchandisingv1.ApprovePlacementResponse{
		Placement: &merchandisingv1.Placement{
			Status: normalizePlacementReply.GetPlacement().GetStatus(),
			Audit: &commonv1.AuditInfo{
				CreatedAt: createNotificationReply.GetNotification().GetAudit().GetCreatedAt(),
			},
			Participants: []*merchandisingv1.PlacementParticipant{&merchandisingv1.PlacementParticipant{
				Contact: &commonv1.ContactInfo{
					Name: createNotificationReply.GetNotification().GetRequester().GetName(),
				},
			}},
			PlacementId: req.GetPlacementId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: countPlacementsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &merchandisingv1.ApprovePlacementResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// RejectPlacement rejects a placement.
func (s *PlacementApprovalServer) RejectPlacement(ctx context.Context, req *merchandisingv1.RejectPlacementRequest) (*merchandisingv1.RejectPlacementResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePlacement(ctx, req.GetContext().GetTenantId(), req.GetPlacement()); err != nil {
		return nil, grpcx.Wrap(err, "save placement")
	}
	s.log.InfoContext(ctx, "reject placement", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPlacement().GetAudit().GetEtag())
	exportPlacementsStream, err := s.placementQueryClient.ExportPlacements(ctx, &merchandisingv1.ExportPlacementsRequest{
		Context: req.GetContext(),
		Filter:  &merchandisingv1.ExportPlacementsRequest_Filter{Region: req.GetPlacement().GetLocation().GetRegion()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementQueryService/ExportPlacements")
	}
	for {
		received, err := exportPlacementsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementQueryService/ExportPlacements")
		}
		_ = received
	}
	jobs := make(chan *merchandisingv1.PlacementParticipant)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for participant := range jobs {
				if _, err := s.sellerScorecardApprovalClient.SubmitSellerScorecard(workCtx, &sellersv1.SubmitSellerScorecardRequest{
					Context: req.GetContext(),
					SellerScorecard: &sellersv1.SellerScorecard{
						Counterparty:   &customersv1.CustomerRef{Email: participant.GetContact().GetEmail()},
						BillingAddress: &commonv1.PostalAddress{Region: req.GetPlacement().GetLocation().GetRegion()},
					},
					Details: &sellersv1.SubmitSellerScorecardRequest_Details{
						Approver: &identityv1.UserRef{Email: req.GetDetails().GetApprover().GetEmail()},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetPlacement().GetParticipants() {
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
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardApprovalService/SubmitSellerScorecard")
	}
	previewMetricSeriesReply, err := s.metricSeriesValidationClient.PreviewMetricSeries(ctx, &metricsv1.PreviewMetricSeriesRequest{Context: req.GetContext()})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesValidationService/PreviewMetricSeries")
	}
	return &merchandisingv1.RejectPlacementResponse{
		Placement: &merchandisingv1.Placement{
			Location: &commonv1.PostalAddress{
				PostalCode: previewMetricSeriesReply.GetMetricSeries().GetAddress().GetPostalCode(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: previewMetricSeriesReply.GetMetricSeries().GetAudit().GetCreatedBy(),
			},
			PlacementId: req.GetPlacementId(),
			Metadata:    req.GetPlacement().GetMetadata(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListPendingPlacements lists placements a page at a time.
func (s *PlacementApprovalServer) ListPendingPlacements(ctx context.Context, req *merchandisingv1.ListPendingPlacementsRequest) (*merchandisingv1.ListPendingPlacementsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list pending placements", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	summarizeVideoVisitsReply, err := s.videoVisitQueryClient.SummarizeVideoVisits(ctx, &telehealthv1.SummarizeVideoVisitsRequest{
		Context:     req.GetContext(),
		Details:     &telehealthv1.SummarizeVideoVisitsRequest_Details{Filters: req.GetFilter().GetLabels()},
		Annotations: baggage,
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.telehealth.v1.VideoVisitQueryService/SummarizeVideoVisits")
	}
	return &merchandisingv1.ListPendingPlacementsResponse{
		Result: &commonv1.OperationResult{
			Message: summarizeVideoVisitsReply.GetResult().GetMessage(),
			Details: summarizeVideoVisitsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// DelegatePlacementApproval delegates a placement.
func (s *PlacementApprovalServer) DelegatePlacementApproval(ctx context.Context, req *merchandisingv1.DelegatePlacementApprovalRequest) (*merchandisingv1.DelegatePlacementApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePlacement(ctx, req.GetContext().GetTenantId(), req.GetPlacement()); err != nil {
		return nil, grpcx.Wrap(err, "save placement")
	}
	s.log.InfoContext(ctx, "delegate placement approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPlacement().GetAudit().GetEtag())
	sendSearchIndexTestNoticeRequest := buildSendSearchIndexTestNoticeRequestForDelegatePlacementApproval(req)
	sendSearchIndexTestNoticeReply, err := s.searchIndexNotificationClient.SendSearchIndexTestNotice(ctx, sendSearchIndexTestNoticeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/SendSearchIndexTestNotice")
	}
	return &merchandisingv1.DelegatePlacementApprovalResponse{
		Outcome: &merchandisingv1.DelegatePlacementApprovalResponse_Outcome{
			Message: sendSearchIndexTestNoticeReply.GetResult().GetMessage(),
		},
		Placement: &merchandisingv1.Placement{
			PlacementId: req.GetPlacementId(),
			Metadata:    req.GetPlacement().GetMetadata(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecallPlacement recalls a placement.
func (s *PlacementApprovalServer) RecallPlacement(ctx context.Context, req *merchandisingv1.RecallPlacementRequest) (*merchandisingv1.RecallPlacementResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePlacement(ctx, req.GetContext().GetTenantId(), req.GetPlacement()); err != nil {
		return nil, grpcx.Wrap(err, "save placement")
	}
	s.log.InfoContext(ctx, "recall placement", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPlacement().GetAudit().GetEtag())
	var holder struct{ Region string }
	if record := req.GetPlacement().GetLocation(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Region"); err != nil {
			s.log.WarnContext(ctx, "copy region", "error", err)
		}
	}
	listApiCredentialVersionsRequest := &identityv1.ListApiCredentialVersionsRequest{
		Context: req.GetContext(),
		Filter:  &identityv1.ListApiCredentialVersionsRequest_Filter{Region: holder.Region},
	}
	send := s.listApiCredentialVersionsSender(listApiCredentialVersionsRequest)
	listApiCredentialVersionsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialHistoryService/ListApiCredentialVersions")
	}
	return &merchandisingv1.RecallPlacementResponse{
		Result: &commonv1.OperationResult{
			Status: listApiCredentialVersionsReply.GetResult().GetStatus(),
		},
		Outcome: &merchandisingv1.RecallPlacementResponse_Outcome{
			Status: listApiCredentialVersionsReply.GetResult().GetStatus(),
		},
		Placement: &merchandisingv1.Placement{
			PlacementId: req.GetPlacementId(),
			Organizer:   req.GetPlacement().GetOrganizer(),
		},
	}, nil
}

// GetPlacementApprovalStatus returns a placement.
func (s *PlacementApprovalServer) GetPlacementApprovalStatus(ctx context.Context, req *merchandisingv1.GetPlacementApprovalStatusRequest) (*merchandisingv1.GetPlacementApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetPlacementApprovalStatus", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "placement", req.GetPlacementId(), "get_placement_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get placement approval status", "tenant", req.GetContext().GetTenantId())
	return &merchandisingv1.GetPlacementApprovalStatusResponse{
		Placement: &merchandisingv1.Placement{PlacementId: req.GetPlacementId()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// collectionQueryLazy dials acme.merchandising.v1.CollectionQueryService the first time a handler needs it.
func (s *PlacementApprovalServer) collectionQueryLazy() merchandisingv1.CollectionQueryServiceClient {
	s.collectionQueryOnce.Do(func() { s.collectionQueryCached = merchandisingv1.NewCollectionQueryServiceClient(s.conn) })
	return s.collectionQueryCached
}

// listApiCredentialVersionsSender returns a function that sends one request to
// acme.identity.v1.ApiCredentialHistoryService/ListApiCredentialVersions.
func (s *PlacementApprovalServer) listApiCredentialVersionsSender(request *identityv1.ListApiCredentialVersionsRequest) func(context.Context) (*identityv1.ListApiCredentialVersionsResponse, error) {
	return func(ctx context.Context) (*identityv1.ListApiCredentialVersionsResponse, error) {
		return s.apiCredentialHistoryClient.ListApiCredentialVersions(ctx, request)
	}
}
