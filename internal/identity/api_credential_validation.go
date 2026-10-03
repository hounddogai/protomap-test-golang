package identity

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ApiCredentialValidationServer implements `acme.identity.v1.ApiCredentialValidationService`.
// Checks api credentials against the rules before anything acts on them.
type ApiCredentialValidationServer struct {
	identityv1.UnimplementedApiCredentialValidationServiceServer
	store                     *Store
	log                       *slog.Logger
	featureFlagClient         featureflagsv1.FeatureFlagServiceClient
	sessionNotificationClient identityv1.SessionNotificationServiceClient
}

// NewApiCredentialValidationServer wires an ApiCredentialValidationServer.
func NewApiCredentialValidationServer(deps platform.Deps) *ApiCredentialValidationServer {
	return &ApiCredentialValidationServer{
		store:                     NewStore(deps.DB),
		log:                       deps.Logger.With("service", "acme.identity.v1.ApiCredentialValidationService"),
		featureFlagClient:         featureflagsv1.NewFeatureFlagServiceClient(deps.Conn),
		sessionNotificationClient: identityv1.NewSessionNotificationServiceClient(deps.Conn),
	}
}

// ValidateApiCredential validates an api credential.
func (s *ApiCredentialValidationServer) ValidateApiCredential(ctx context.Context, req *identityv1.ValidateApiCredentialRequest) (*identityv1.ValidateApiCredentialResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveApiCredential(ctx, req.GetContext().GetTenantId(), req.GetApiCredential()); err != nil {
		return nil, grpcx.Wrap(err, "save api credential")
	}
	s.log.InfoContext(ctx, "validate api credential", "tenant", req.GetContext().GetTenantId(), "etag", req.GetApiCredential().GetAudit().GetEtag())
	labels := make(map[string]string, len(req.GetApiCredential().GetLabels()))
	for key, value := range req.GetApiCredential().GetLabels() {
		labels[key] = value
	}
	listFeatureFlagsRequest := &featureflagsv1.ListFeatureFlagsRequest{}
	listFeatureFlagsRequest.Context = req.GetContext()
	listFeatureFlagsRequest.Filter = &featureflagsv1.ListFeatureFlagsRequest_Filter{
		Labels:       labels,
		CreatedAfter: req.GetApiCredential().GetAudit().GetCreatedAt(),
		OwnerId:      req.GetApiCredential().GetHolder().GetSessionId(),
	}
	listFeatureFlagsReply, err := s.featureFlagClient.ListFeatureFlags(ctx, listFeatureFlagsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagService/ListFeatureFlags")
	}
	return &identityv1.ValidateApiCredentialResponse{
		Outcome: &identityv1.ValidateApiCredentialResponse_Outcome{Status: listFeatureFlagsReply.GetResult().GetStatus()},
		ApiCredential: &identityv1.ApiCredential{
			ApiCredentialId: req.GetApiCredentialId(),
			Available:       req.GetApiCredential().GetAvailable(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PreviewApiCredential previews an api credential.
func (s *ApiCredentialValidationServer) PreviewApiCredential(ctx context.Context, req *identityv1.PreviewApiCredentialRequest) (*identityv1.PreviewApiCredentialResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveApiCredential(ctx, req.GetContext().GetTenantId(), req.GetApiCredential()); err != nil {
		return nil, grpcx.Wrap(err, "save api credential")
	}
	s.log.InfoContext(ctx, "preview api credential", "tenant", req.GetContext().GetTenantId(), "etag", req.GetApiCredential().GetAudit().GetEtag())
	streamSessionNoticesStream, err := s.sessionNotificationClient.StreamSessionNotices(ctx, &identityv1.StreamSessionNoticesRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email:            req.GetDetails().GetContact().GetEmail(),
			Phone:            req.GetDetails().GetContact().GetPhone(),
			PreferredChannel: req.GetApiCredential().GetContact().GetPreferredChannel(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/StreamSessionNotices")
	}
	for {
		received, err := streamSessionNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/StreamSessionNotices")
		}
		_ = received
	}
	return &identityv1.PreviewApiCredentialResponse{
		ApiCredential: &identityv1.ApiCredential{
			ApiCredentialId: req.GetApiCredentialId(),
			Holder:          req.GetApiCredential().GetHolder(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckApiCredentialEligibility checks an api credential.
func (s *ApiCredentialValidationServer) CheckApiCredentialEligibility(ctx context.Context, req *identityv1.CheckApiCredentialEligibilityRequest) (*identityv1.CheckApiCredentialEligibilityResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveApiCredential(ctx, req.GetContext().GetTenantId(), req.GetApiCredential()); err != nil {
		return nil, grpcx.Wrap(err, "save api credential")
	}
	s.log.InfoContext(ctx, "check api credential eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetApiCredential().GetAudit().GetEtag())
	return &identityv1.CheckApiCredentialEligibilityResponse{
		ApiCredential: &identityv1.ApiCredential{
			ApiCredentialId: req.GetApiCredentialId(),
			CurrencyCode:    req.GetApiCredential().GetCurrencyCode(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizeApiCredential normalizes an api credential.
func (s *ApiCredentialValidationServer) NormalizeApiCredential(ctx context.Context, req *identityv1.NormalizeApiCredentialRequest) (*identityv1.NormalizeApiCredentialResponse, error) {
	if err := s.store.SaveApiCredential(ctx, req.GetContext().GetTenantId(), req.GetApiCredential()); err != nil {
		return nil, grpcx.Wrap(err, "save api credential")
	}
	s.log.InfoContext(ctx, "normalize api credential", "tenant", req.GetContext().GetTenantId(), "etag", req.GetApiCredential().GetAudit().GetEtag())
	return &identityv1.NormalizeApiCredentialResponse{
		ApiCredential: &identityv1.ApiCredential{
			ApiCredentialId: req.GetApiCredentialId(),
			Contact:         req.GetApiCredential().GetContact(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainApiCredentialDecision explains an api credential.
func (s *ApiCredentialValidationServer) ExplainApiCredentialDecision(ctx context.Context, req *identityv1.ExplainApiCredentialDecisionRequest) (*identityv1.ExplainApiCredentialDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainApiCredentialDecision", "error", err)
		return nil, err
	}
	if err := s.store.SaveApiCredential(ctx, req.GetContext().GetTenantId(), req.GetApiCredential()); err != nil {
		return nil, grpcx.Wrap(err, "save api credential")
	}
	s.log.InfoContext(ctx, "explain api credential decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetApiCredential().GetAudit().GetEtag())
	return &identityv1.ExplainApiCredentialDecisionResponse{
		ApiCredential: &identityv1.ApiCredential{
			ApiCredentialId: req.GetApiCredentialId(),
			Balance:         req.GetApiCredential().GetBalance(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScoreApiCredential scores an api credential.
func (s *ApiCredentialValidationServer) ScoreApiCredential(ctx context.Context, req *identityv1.ScoreApiCredentialRequest) (*identityv1.ScoreApiCredentialResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScoreApiCredential", "error", err)
		return nil, err
	}
	if err := s.store.SaveApiCredential(ctx, req.GetContext().GetTenantId(), req.GetApiCredential()); err != nil {
		return nil, grpcx.Wrap(err, "save api credential")
	}
	s.log.InfoContext(ctx, "score api credential", "tenant", req.GetContext().GetTenantId(), "etag", req.GetApiCredential().GetAudit().GetEtag())
	return &identityv1.ScoreApiCredentialResponse{
		ApiCredential: &identityv1.ApiCredential{ApiCredentialId: req.GetApiCredentialId()},
		Breakdown:     &identityv1.ScoreApiCredentialResponse_Breakdown{CurrencyCode: req.GetApiCredential().GetCurrencyCode()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LintApiCredentials lints an api credential.
func (s *ApiCredentialValidationServer) LintApiCredentials(ctx context.Context, req *identityv1.LintApiCredentialsRequest) (*identityv1.LintApiCredentialsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LintApiCredentials", "error", err)
		return nil, err
	}
	if err := s.store.SaveApiCredential(ctx, req.GetContext().GetTenantId(), req.GetApiCredential()); err != nil {
		return nil, grpcx.Wrap(err, "save api credential")
	}
	s.log.InfoContext(ctx, "lint api credentials", "tenant", req.GetContext().GetTenantId(), "etag", req.GetApiCredential().GetAudit().GetEtag())
	return &identityv1.LintApiCredentialsResponse{
		ApiCredential: &identityv1.ApiCredential{
			ApiCredentialId: req.GetApiCredentialId(),
			Contact:         req.GetDetails().GetContact(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetApiCredentialRules returns an api credential.
func (s *ApiCredentialValidationServer) GetApiCredentialRules(ctx context.Context, req *identityv1.GetApiCredentialRulesRequest) (*identityv1.GetApiCredentialRulesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "api_credential", req.GetApiCredentialId(), "get_api_credential_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get api credential rules", "tenant", req.GetContext().GetTenantId())
	return &identityv1.GetApiCredentialRulesResponse{
		ApiCredential: &identityv1.ApiCredential{ApiCredentialId: req.GetApiCredentialId()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}
