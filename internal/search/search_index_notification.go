package search

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc"
)

// SearchIndexNotificationServer implements `acme.search.v1.SearchIndexNotificationService`.
// Tells the people who follow a search index what changed.
type SearchIndexNotificationServer struct {
	searchv1.UnimplementedSearchIndexNotificationServiceServer
	store *Store
	log   *slog.Logger
}

// NewSearchIndexNotificationServer wires a SearchIndexNotificationServer.
func NewSearchIndexNotificationServer(deps platform.Deps) *SearchIndexNotificationServer {
	return &SearchIndexNotificationServer{
		store: NewStore(deps.DB),
		log:   deps.Logger.With("service", "acme.search.v1.SearchIndexNotificationService"),
	}
}

// SubscribeSearchIndexUpdates subscribes a search index.
func (s *SearchIndexNotificationServer) SubscribeSearchIndexUpdates(ctx context.Context, req *searchv1.SubscribeSearchIndexUpdatesRequest) (*searchv1.SubscribeSearchIndexUpdatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_index", req.GetSearchIndexId(), "subscribe_search_index_updates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "subscribe search index updates", "tenant", req.GetContext().GetTenantId())
	return &searchv1.SubscribeSearchIndexUpdatesResponse{
		SearchIndex: &searchv1.SearchIndex{
			SearchIndexId: req.GetSearchIndexId(),
			Owner:         &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// UnsubscribeSearchIndexUpdates unsubscribes a search index.
func (s *SearchIndexNotificationServer) UnsubscribeSearchIndexUpdates(ctx context.Context, req *searchv1.UnsubscribeSearchIndexUpdatesRequest) (*searchv1.UnsubscribeSearchIndexUpdatesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_index", req.GetSearchIndexId(), "unsubscribe_search_index_updates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "unsubscribe search index updates", "tenant", req.GetContext().GetTenantId())
	return &searchv1.UnsubscribeSearchIndexUpdatesResponse{
		SearchIndex: &searchv1.SearchIndex{
			SearchIndexId: req.GetSearchIndexId(),
			Owner:         &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NotifySearchIndexWatchers notifys a search index.
func (s *SearchIndexNotificationServer) NotifySearchIndexWatchers(ctx context.Context, req *searchv1.NotifySearchIndexWatchersRequest) (*searchv1.NotifySearchIndexWatchersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected NotifySearchIndexWatchers", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_index", req.GetSearchIndexId(), "notify_search_index_watchers"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "notify search index watchers", "tenant", req.GetContext().GetTenantId())
	return &searchv1.NotifySearchIndexWatchersResponse{
		SearchIndex: &searchv1.SearchIndex{
			SearchIndexId: req.GetSearchIndexId(),
			Owner:         &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListSearchIndexSubscriptions lists search indexes a page at a time.
func (s *SearchIndexNotificationServer) ListSearchIndexSubscriptions(ctx context.Context, req *searchv1.ListSearchIndexSubscriptionsRequest) (*searchv1.ListSearchIndexSubscriptionsResponse, error) {
	s.log.InfoContext(ctx, "list search index subscriptions", "tenant", req.GetContext().GetTenantId())
	return &searchv1.ListSearchIndexSubscriptionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SendSearchIndexTestNotice sends a search index.
func (s *SearchIndexNotificationServer) SendSearchIndexTestNotice(ctx context.Context, req *searchv1.SendSearchIndexTestNoticeRequest) (*searchv1.SendSearchIndexTestNoticeResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_index", req.GetSearchIndexId(), "send_search_index_test_notice"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "send search index test notice", "tenant", req.GetContext().GetTenantId())
	return &searchv1.SendSearchIndexTestNoticeResponse{
		SearchIndex: &searchv1.SearchIndex{
			SearchIndexId: req.GetSearchIndexId(),
			Owner:         &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// StreamSearchIndexNotices streams the changes of search indexes as they happen.
func (s *SearchIndexNotificationServer) StreamSearchIndexNotices(req *searchv1.StreamSearchIndexNoticesRequest, stream grpc.ServerStreamingServer[searchv1.StreamSearchIndexNoticesResponse]) error {
	ctx := stream.Context()
	s.log.InfoContext(ctx, "stream search index notices", "tenant", req.GetContext().GetTenantId())
	if err := stream.Send(&searchv1.StreamSearchIndexNoticesResponse{
		SearchIndex: &searchv1.SearchIndex{
			Owner: &identityv1.UserRef{Email: req.GetSubscriber().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}); err != nil {
		return err
	}
	return nil
}
