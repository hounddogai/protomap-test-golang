package identity

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SessionQueryServer implements `acme.identity.v1.SessionQueryService`.
// Answers read-only questions about sessions, for screens and reports.
type SessionQueryServer struct {
	identityv1.UnimplementedSessionQueryServiceServer
	store             *Store
	log               *slog.Logger
	venueQueryClient  venuesv1.VenueQueryServiceClient
	tenantAdminClient identityv1.TenantAdminServiceClient
}

// NewSessionQueryServer wires a SessionQueryServer.
func NewSessionQueryServer(deps platform.Deps) *SessionQueryServer {
	return &SessionQueryServer{
		store:             NewStore(deps.DB),
		log:               deps.Logger.With("service", "acme.identity.v1.SessionQueryService"),
		venueQueryClient:  venuesv1.NewVenueQueryServiceClient(deps.Conn),
		tenantAdminClient: identityv1.NewTenantAdminServiceClient(deps.Conn),
	}
}

// LookupSession returns a session.
func (s *SessionQueryServer) LookupSession(ctx context.Context, req *identityv1.LookupSessionRequest) (*identityv1.LookupSessionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "session", req.GetSessionId(), "lookup_session"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup session", "tenant", req.GetContext().GetTenantId())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	countVenuesReply, err := s.venueQueryClient.CountVenues(ctx, &venuesv1.CountVenuesRequest{
		Context: req.GetContext(),
		Filter: &venuesv1.CountVenuesRequest_Filter{
			OwnerId:      req.GetSessionId(),
			CreatedAfter: cachedContext.GetDeadline(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueQueryService/CountVenues")
	}
	return &identityv1.LookupSessionResponse{
		Result: &commonv1.OperationResult{
			Details: countVenuesReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Session: &identityv1.Session{SessionId: req.GetSessionId()},
	}, nil
}

// BrowseSessions lists sessions a page at a time.
func (s *SessionQueryServer) BrowseSessions(ctx context.Context, req *identityv1.BrowseSessionsRequest) (*identityv1.BrowseSessionsResponse, error) {
	s.log.InfoContext(ctx, "browse sessions", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	unlockTenantRequest := &identityv1.UnlockTenantRequest{}
	unlockTenantRequest.Context = req.GetContext()
	unlockTenantRequest.TenantId = req.GetContext().GetTenantId()
	unlockTenantRequest.Details = &identityv1.UnlockTenantRequest_Details{
		NewOwner: &identityv1.UserRef{
			Email:  tenancy.Value(ctx, "email"),
			UserId: req.GetContext().GetActor().GetUserId(),
		},
	}
	unlockTenantReply, err := s.tenantAdminClient.UnlockTenant(ctx, unlockTenantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/UnlockTenant")
	}
	return &identityv1.BrowseSessionsResponse{
		Result: &commonv1.OperationResult{Status: unlockTenantReply.GetOutcome().GetStatus()},
	}, nil
}

// FindSessions lists sessions a page at a time.
func (s *SessionQueryServer) FindSessions(ctx context.Context, req *identityv1.FindSessionsRequest) (*identityv1.FindSessionsResponse, error) {
	s.log.InfoContext(ctx, "find sessions", "tenant", req.GetContext().GetTenantId())
	return &identityv1.FindSessionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// CountSessions counts sessions by status.
func (s *SessionQueryServer) CountSessions(ctx context.Context, req *identityv1.CountSessionsRequest) (*identityv1.CountSessionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CountSessions", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "count sessions", "tenant", req.GetContext().GetTenantId())
	return &identityv1.CountSessionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ExportSessions streams every matching session.
func (s *SessionQueryServer) ExportSessions(req *identityv1.ExportSessionsRequest, stream grpc.ServerStreamingServer[identityv1.ExportSessionsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExportSessions", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "export sessions", "tenant", req.GetContext().GetTenantId())
	if err := stream.Send(&identityv1.ExportSessionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}); err != nil {
		return err
	}
	return nil
}

// SummarizeSessions summarizes a session.
func (s *SessionQueryServer) SummarizeSessions(ctx context.Context, req *identityv1.SummarizeSessionsRequest) (*identityv1.SummarizeSessionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SummarizeSessions", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "session", req.GetSessionId(), "summarize_sessions"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize sessions", "tenant", req.GetContext().GetTenantId())
	return &identityv1.SummarizeSessionsResponse{
		Period: req.GetDetails().GetPeriod(),
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SuggestSessions lists sessions a page at a time.
func (s *SessionQueryServer) SuggestSessions(ctx context.Context, req *identityv1.SuggestSessionsRequest) (*identityv1.SuggestSessionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuggestSessions", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "suggest sessions", "tenant", req.GetContext().GetTenantId())
	return &identityv1.SuggestSessionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}
