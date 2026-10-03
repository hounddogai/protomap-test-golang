package identity

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SessionValidationServer implements `acme.identity.v1.SessionValidationService`.
// Checks sessions against the rules before anything acts on them.
type SessionValidationServer struct {
	identityv1.UnimplementedSessionValidationServiceServer
	store *Store
	log   *slog.Logger
	conn  *grpc.ClientConn
	bus   *eventbus.Bus
}

// NewSessionValidationServer wires a SessionValidationServer.
func NewSessionValidationServer(deps platform.Deps) *SessionValidationServer {
	return &SessionValidationServer{
		store: NewStore(deps.DB),
		log:   deps.Logger.With("service", "acme.identity.v1.SessionValidationService"),
		conn:  deps.Conn,
		bus:   deps.Bus,
	}
}

// ValidateSession validates a session.
func (s *SessionValidationServer) ValidateSession(ctx context.Context, req *identityv1.ValidateSessionRequest) (*identityv1.ValidateSessionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSession(ctx, req.GetContext().GetTenantId(), req.GetSession()); err != nil {
		return nil, grpcx.Wrap(err, "save session")
	}
	s.log.InfoContext(ctx, "validate session", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSession().GetAudit().GetEtag())
	return &identityv1.ValidateSessionResponse{
		Session: &identityv1.Session{
			SessionId: req.GetSessionId(),
			Status:    req.GetSession().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PreviewSession previews a session.
func (s *SessionValidationServer) PreviewSession(ctx context.Context, req *identityv1.PreviewSessionRequest) (*identityv1.PreviewSessionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSession(ctx, req.GetContext().GetTenantId(), req.GetSession()); err != nil {
		return nil, grpcx.Wrap(err, "save session")
	}
	s.log.InfoContext(ctx, "preview session", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSession().GetAudit().GetEtag())
	return &identityv1.PreviewSessionResponse{
		Session: &identityv1.Session{
			SessionId: req.GetSessionId(),
			Subject:   req.GetSession().GetSubject(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckSessionEligibility checks a session.
func (s *SessionValidationServer) CheckSessionEligibility(ctx context.Context, req *identityv1.CheckSessionEligibilityRequest) (*identityv1.CheckSessionEligibilityResponse, error) {
	if err := s.store.SaveSession(ctx, req.GetContext().GetTenantId(), req.GetSession()); err != nil {
		return nil, grpcx.Wrap(err, "save session")
	}
	s.log.InfoContext(ctx, "check session eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSession().GetAudit().GetEtag())
	normalizeDocumentRequest := &documentsv1.NormalizeDocumentRequest{
		Context:  req.GetContext(),
		Document: &documentsv1.Document{DueAt: req.GetSession().GetSla().GetDueAt()},
		Details: &documentsv1.NormalizeDocumentRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: req.GetSession().GetRequester().GetEmail(),
				Name:  req.GetDetails().GetContact().GetName(),
			},
		},
	}
	normalizeDocumentReply := new(documentsv1.NormalizeDocumentResponse)
	if err := grpcx.Invoke(ctx, s.conn, documentsv1.DocumentValidationService_NormalizeDocument_FullMethodName, normalizeDocumentRequest, normalizeDocumentReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentValidationService/NormalizeDocument")
	}
	return &identityv1.CheckSessionEligibilityResponse{
		Session: &identityv1.Session{
			Sla:       &identityv1.SessionSla{DueAt: normalizeDocumentReply.GetDocument().GetDueAt()},
			SessionId: req.GetSessionId(),
			Subject:   req.GetSession().GetSubject(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainSessionDecision explains a session.
func (s *SessionValidationServer) ExplainSessionDecision(ctx context.Context, req *identityv1.ExplainSessionDecisionRequest) (*identityv1.ExplainSessionDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainSessionDecision", "error", err)
		return nil, err
	}
	if err := s.store.SaveSession(ctx, req.GetContext().GetTenantId(), req.GetSession()); err != nil {
		return nil, grpcx.Wrap(err, "save session")
	}
	s.log.InfoContext(ctx, "explain session decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSession().GetAudit().GetEtag())
	return &identityv1.ExplainSessionDecisionResponse{
		Session: &identityv1.Session{
			SessionId: req.GetSessionId(),
			Subject:   req.GetSession().GetSubject(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScoreSession scores a session.
func (s *SessionValidationServer) ScoreSession(ctx context.Context, req *identityv1.ScoreSessionRequest) (*identityv1.ScoreSessionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScoreSession", "error", err)
		return nil, err
	}
	if err := s.store.SaveSession(ctx, req.GetContext().GetTenantId(), req.GetSession()); err != nil {
		return nil, grpcx.Wrap(err, "save session")
	}
	s.log.InfoContext(ctx, "score session", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSession().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "identity.score_session.compare_session_versions",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"session.channel.channel": req.GetSession().GetChannel().GetChannel(),
			"session.requester.email": req.GetSession().GetAssignee().GetEmail(),
			"context.tenant_id":       req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &identityv1.ScoreSessionResponse{
		Session:   &identityv1.Session{SessionId: req.GetSessionId()},
		Breakdown: &identityv1.ScoreSessionResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LintSessions lints a session.
func (s *SessionValidationServer) LintSessions(ctx context.Context, req *identityv1.LintSessionsRequest) (*identityv1.LintSessionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LintSessions", "error", err)
		return nil, err
	}
	if err := s.store.SaveSession(ctx, req.GetContext().GetTenantId(), req.GetSession()); err != nil {
		return nil, grpcx.Wrap(err, "save session")
	}
	s.log.InfoContext(ctx, "lint sessions", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSession().GetAudit().GetEtag())
	return &identityv1.LintSessionsResponse{
		Session: &identityv1.Session{
			SessionId: req.GetSessionId(),
			Subject:   req.GetSession().GetSubject(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetSessionRules returns a session.
func (s *SessionValidationServer) GetSessionRules(ctx context.Context, req *identityv1.GetSessionRulesRequest) (*identityv1.GetSessionRulesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "session", req.GetSessionId(), "get_session_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get session rules", "tenant", req.GetContext().GetTenantId())
	return &identityv1.GetSessionRulesResponse{
		Session: &identityv1.Session{SessionId: req.GetSessionId()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}
