package search

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// SearchQueryNotificationServer implements `acme.search.v1.SearchQueryNotificationService`.
// Tells the people who follow a search query what changed.
type SearchQueryNotificationServer struct {
	searchv1.UnimplementedSearchQueryNotificationServiceServer
	store                          *Store
	log                            *slog.Logger
	synonymSyncClient              searchv1.SynonymSyncServiceClient
	meteredSearchIndexNotification meteredSearchIndexNotification
}

// NewSearchQueryNotificationServer wires a SearchQueryNotificationServer.
func NewSearchQueryNotificationServer(deps platform.Deps) *SearchQueryNotificationServer {
	return &SearchQueryNotificationServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.search.v1.SearchQueryNotificationService"),
		synonymSyncClient:              searchv1.NewSynonymSyncServiceClient(deps.Conn),
		meteredSearchIndexNotification: meteredSearchIndexNotification{SearchIndexNotificationServiceClient: searchv1.NewSearchIndexNotificationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}
}

// SubscribeSearchQueryUpdates subscribes a search query.
func (s *SearchQueryNotificationServer) SubscribeSearchQueryUpdates(ctx context.Context, req *searchv1.SubscribeSearchQueryUpdatesRequest) (*searchv1.SubscribeSearchQueryUpdatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubscribeSearchQueryUpdates", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_query", req.GetSearchQueryId(), "subscribe_search_query_updates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "subscribe search query updates", "tenant", req.GetContext().GetTenantId())
	return &searchv1.SubscribeSearchQueryUpdatesResponse{
		SearchQuery: &searchv1.SearchQuery{SearchQueryId: req.GetSearchQueryId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// UnsubscribeSearchQueryUpdates unsubscribes a search query.
func (s *SearchQueryNotificationServer) UnsubscribeSearchQueryUpdates(ctx context.Context, req *searchv1.UnsubscribeSearchQueryUpdatesRequest) (*searchv1.UnsubscribeSearchQueryUpdatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected UnsubscribeSearchQueryUpdates", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_query", req.GetSearchQueryId(), "unsubscribe_search_query_updates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "unsubscribe search query updates", "tenant", req.GetContext().GetTenantId())
	return &searchv1.UnsubscribeSearchQueryUpdatesResponse{
		SearchQuery: &searchv1.SearchQuery{SearchQueryId: req.GetSearchQueryId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NotifySearchQueryWatchers notifys a search query.
func (s *SearchQueryNotificationServer) NotifySearchQueryWatchers(ctx context.Context, req *searchv1.NotifySearchQueryWatchersRequest) (*searchv1.NotifySearchQueryWatchersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected NotifySearchQueryWatchers", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_query", req.GetSearchQueryId(), "notify_search_query_watchers"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "notify search query watchers", "tenant", req.GetContext().GetTenantId())
	return &searchv1.NotifySearchQueryWatchersResponse{
		SearchQuery: &searchv1.SearchQuery{SearchQueryId: req.GetSearchQueryId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListSearchQuerySubscriptions lists search queries a page at a time.
func (s *SearchQueryNotificationServer) ListSearchQuerySubscriptions(ctx context.Context, req *searchv1.ListSearchQuerySubscriptionsRequest) (*searchv1.ListSearchQuerySubscriptionsResponse, error) {
	s.log.InfoContext(ctx, "list search query subscriptions", "tenant", req.GetContext().GetTenantId())
	syncSynonymsReply, err := s.synonymSyncClient.SyncSynonyms(ctx, &searchv1.SyncSynonymsRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetFilter().GetCreatedAfter()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/SyncSynonyms")
	}
	return &searchv1.ListSearchQuerySubscriptionsResponse{
		Result: &commonv1.OperationResult{Status: syncSynonymsReply.GetResult().GetStatus()},
	}, nil
}

// SendSearchQueryTestNotice sends a search query.
func (s *SearchQueryNotificationServer) SendSearchQueryTestNotice(ctx context.Context, req *searchv1.SendSearchQueryTestNoticeRequest) (*searchv1.SendSearchQueryTestNoticeResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_query", req.GetSearchQueryId(), "send_search_query_test_notice"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "send search query test notice", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*searchv1.SendSearchQueryTestNoticeRequest_Details)
	ctx = tenancy.With(ctx, "phone", req.GetDetails().GetRecipient().GetPhone())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded searchv1.SendSearchQueryTestNoticeRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	sendSearchIndexTestNoticeRequest := &searchv1.SendSearchIndexTestNoticeRequest{
		Context: req.GetContext(),
		Details: &searchv1.SendSearchIndexTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email: cloned.GetRecipient().GetEmail(),
				Phone: tenancy.Value(ctx, "phone"),
				Name:  (&decoded).GetRecipient().GetName(),
			},
			Variables: req.GetDetails().GetVariables(),
		},
	}
	sendSearchIndexTestNoticeReply, err := s.meteredSearchIndexNotification.SendSearchIndexTestNotice(ctx, sendSearchIndexTestNoticeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/SendSearchIndexTestNotice")
	}
	return &searchv1.SendSearchQueryTestNoticeResponse{
		SearchQuery: &searchv1.SearchQuery{
			Subject: &searchv1.SearchIndexRef{
				Code: sendSearchIndexTestNoticeReply.GetSearchIndex().GetCode(),
			},
			SearchQueryId: req.GetSearchQueryId(),
		},
		Outcome: &searchv1.SendSearchQueryTestNoticeResponse_Outcome{
			Status: sendSearchIndexTestNoticeReply.GetOutcome().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetSearchQueryPreferences returns a search query.
func (s *SearchQueryNotificationServer) GetSearchQueryPreferences(ctx context.Context, req *searchv1.GetSearchQueryPreferencesRequest) (*searchv1.GetSearchQueryPreferencesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetSearchQueryPreferences", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_query", req.GetSearchQueryId(), "get_search_query_preferences"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get search query preferences", "tenant", req.GetContext().GetTenantId())
	return &searchv1.GetSearchQueryPreferencesResponse{
		SearchQuery: &searchv1.SearchQuery{SearchQueryId: req.GetSearchQueryId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// StreamSearchQueryNotices streams the changes of search queries as they happen.
func (s *SearchQueryNotificationServer) StreamSearchQueryNotices(req *searchv1.StreamSearchQueryNoticesRequest, stream grpc.ServerStreamingServer[searchv1.StreamSearchQueryNoticesResponse]) error {
	ctx := stream.Context()
	s.log.InfoContext(ctx, "stream search query notices", "tenant", req.GetContext().GetTenantId())
	streamSynonymChangesStream, err := s.synonymSyncClient.StreamSynonymChanges(ctx)
	if err != nil {
		return grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/StreamSynonymChanges")
	}
	if err := streamSynonymChangesStream.Send(&searchv1.StreamSynonymChangesRequest{
		Context: req.GetContext(),
		Frame: &searchv1.StreamSynonymChangesRequest_Change{Change: &searchv1.Synonym{
			Owner: &identityv1.UserRef{Email: req.GetSubscriber().GetEmail()},
		}},
	}); err != nil {
		return grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/StreamSynonymChanges")
	}
	if err := streamSynonymChangesStream.CloseSend(); err != nil {
		return grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/StreamSynonymChanges")
	}
	acknowledged, err := streamSynonymChangesStream.Recv()
	if err != nil && !errors.Is(err, io.EOF) {
		return grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/StreamSynonymChanges")
	}
	_ = acknowledged
	if err := stream.Send(&searchv1.StreamSearchQueryNoticesResponse{Result: &commonv1.OperationResult{Status: "ok"}}); err != nil {
		return err
	}
	return nil
}
