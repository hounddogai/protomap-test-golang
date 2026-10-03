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
	"google.golang.org/protobuf/encoding/protojson"
)

// SynonymNotificationServer implements `acme.search.v1.SynonymNotificationService`.
// Tells the people who follow a synonym what changed.
type SynonymNotificationServer struct {
	searchv1.UnimplementedSynonymNotificationServiceServer
	store                    *Store
	log                      *slog.Logger
	searchIndexHistoryClient searchv1.SearchIndexHistoryServiceClient
}

// NewSynonymNotificationServer wires a SynonymNotificationServer.
func NewSynonymNotificationServer(deps platform.Deps) *SynonymNotificationServer {
	return &SynonymNotificationServer{
		store:                    NewStore(deps.DB),
		log:                      deps.Logger.With("service", "acme.search.v1.SynonymNotificationService"),
		searchIndexHistoryClient: searchv1.NewSearchIndexHistoryServiceClient(deps.Conn),
	}
}

// SubscribeSynonymUpdates subscribes a synonym.
func (s *SynonymNotificationServer) SubscribeSynonymUpdates(ctx context.Context, req *searchv1.SubscribeSynonymUpdatesRequest) (*searchv1.SubscribeSynonymUpdatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubscribeSynonymUpdates", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "synonym", req.GetSynonymId(), "subscribe_synonym_updates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "subscribe synonym updates", "tenant", req.GetContext().GetTenantId())
	return &searchv1.SubscribeSynonymUpdatesResponse{
		Synonym: &searchv1.Synonym{
			SynonymId: req.GetSynonymId(),
			Owner:     &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// UnsubscribeSynonymUpdates unsubscribes a synonym.
func (s *SynonymNotificationServer) UnsubscribeSynonymUpdates(ctx context.Context, req *searchv1.UnsubscribeSynonymUpdatesRequest) (*searchv1.UnsubscribeSynonymUpdatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "synonym", req.GetSynonymId(), "unsubscribe_synonym_updates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "unsubscribe synonym updates", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	compareSearchIndexVersionsRequest := &searchv1.CompareSearchIndexVersionsRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Owner: &identityv1.UserRef{
				Email:  req.GetDetails().GetRecipient().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
		Annotations: annotations,
		Reason:      req.GetReason(),
		Details:     &searchv1.CompareSearchIndexVersionsRequest_Details{Snapshot: snapshot},
	}
	send := s.compareSearchIndexVersionsSender(compareSearchIndexVersionsRequest)
	compareSearchIndexVersionsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexHistoryService/CompareSearchIndexVersions")
	}
	return &searchv1.UnsubscribeSynonymUpdatesResponse{
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{
				CreatedAt: compareSearchIndexVersionsReply.GetSearchIndex().GetAudit().GetCreatedAt(),
			},
			SynonymId: req.GetSynonymId(),
			Owner:     &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListSynonymSubscriptions lists synonyms a page at a time.
func (s *SynonymNotificationServer) ListSynonymSubscriptions(ctx context.Context, req *searchv1.ListSynonymSubscriptionsRequest) (*searchv1.ListSynonymSubscriptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list synonym subscriptions", "tenant", req.GetContext().GetTenantId())
	return &searchv1.ListSynonymSubscriptionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SendSynonymTestNotice sends a synonym.
func (s *SynonymNotificationServer) SendSynonymTestNotice(ctx context.Context, req *searchv1.SendSynonymTestNoticeRequest) (*searchv1.SendSynonymTestNoticeResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "synonym", req.GetSynonymId(), "send_synonym_test_notice"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "send synonym test notice", "tenant", req.GetContext().GetTenantId())
	return &searchv1.SendSynonymTestNoticeResponse{
		Synonym: &searchv1.Synonym{
			SynonymId: req.GetSynonymId(),
			Owner:     &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetSynonymPreferences returns a synonym.
func (s *SynonymNotificationServer) GetSynonymPreferences(ctx context.Context, req *searchv1.GetSynonymPreferencesRequest) (*searchv1.GetSynonymPreferencesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "synonym", req.GetSynonymId(), "get_synonym_preferences"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get synonym preferences", "tenant", req.GetContext().GetTenantId())
	return &searchv1.GetSynonymPreferencesResponse{
		Synonym: &searchv1.Synonym{SynonymId: req.GetSynonymId()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// StreamSynonymNotices streams the changes of synonyms as they happen.
func (s *SynonymNotificationServer) StreamSynonymNotices(req *searchv1.StreamSynonymNoticesRequest, stream grpc.ServerStreamingServer[searchv1.StreamSynonymNoticesResponse]) error {
	ctx := stream.Context()
	s.log.InfoContext(ctx, "stream synonym notices", "tenant", req.GetContext().GetTenantId())
	if err := stream.Send(&searchv1.StreamSynonymNoticesResponse{
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{Email: req.GetSubscriber().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}); err != nil {
		return err
	}
	return nil
}

// compareSearchIndexVersionsSender returns a function that sends one request to
// acme.search.v1.SearchIndexHistoryService/CompareSearchIndexVersions.
func (s *SynonymNotificationServer) compareSearchIndexVersionsSender(request *searchv1.CompareSearchIndexVersionsRequest) func(context.Context) (*searchv1.CompareSearchIndexVersionsResponse, error) {
	return func(ctx context.Context) (*searchv1.CompareSearchIndexVersionsResponse, error) {
		return s.searchIndexHistoryClient.CompareSearchIndexVersions(ctx, request)
	}
}
