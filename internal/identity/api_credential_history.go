package identity

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ApiCredentialHistoryServer implements `acme.identity.v1.ApiCredentialHistoryService`.
// Keeps every version of an api credential and restores earlier ones.
type ApiCredentialHistoryServer struct {
	identityv1.UnimplementedApiCredentialHistoryServiceServer
	store                            *Store
	log                              *slog.Logger
	featureFlagQueryClient           featureflagsv1.FeatureFlagQueryServiceClient
	meteredApiCredentialNotification meteredApiCredentialNotification
}

// NewApiCredentialHistoryServer wires an ApiCredentialHistoryServer.
func NewApiCredentialHistoryServer(deps platform.Deps) *ApiCredentialHistoryServer {
	return &ApiCredentialHistoryServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.identity.v1.ApiCredentialHistoryService"),
		featureFlagQueryClient:           featureflagsv1.NewFeatureFlagQueryServiceClient(deps.Conn),
		meteredApiCredentialNotification: meteredApiCredentialNotification{ApiCredentialNotificationServiceClient: identityv1.NewApiCredentialNotificationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}
}

// RecordApiCredentialVersion records an api credential.
func (s *ApiCredentialHistoryServer) RecordApiCredentialVersion(ctx context.Context, req *identityv1.RecordApiCredentialVersionRequest) (*identityv1.RecordApiCredentialVersionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveApiCredential(ctx, req.GetContext().GetTenantId(), req.GetApiCredential()); err != nil {
		return nil, grpcx.Wrap(err, "save api credential")
	}
	s.log.InfoContext(ctx, "record api credential version", "tenant", req.GetContext().GetTenantId(), "etag", req.GetApiCredential().GetAudit().GetEtag())
	labels := make(map[string]string, len(req.GetApiCredential().GetLabels()))
	for key, value := range req.GetApiCredential().GetLabels() {
		labels[key] = value
	}
	browseFeatureFlagsRequest := newBrowseFeatureFlagsRequest(withBrowseFeatureFlagsContext(req.GetContext()), withBrowseFeatureFlagsFilter(&featureflagsv1.BrowseFeatureFlagsRequest_Filter{Labels: labels}))
	browseFeatureFlagsReply, err := s.featureFlagQueryClient.BrowseFeatureFlags(ctx, browseFeatureFlagsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/BrowseFeatureFlags")
	}
	return &identityv1.RecordApiCredentialVersionResponse{
		Outcome: &identityv1.RecordApiCredentialVersionResponse_Outcome{Message: browseFeatureFlagsReply.GetResult().GetMessage()},
		Result: &commonv1.OperationResult{
			Message: browseFeatureFlagsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		ApiCredential: &identityv1.ApiCredential{
			ApiCredentialId: req.GetApiCredentialId(),
			OpenedAt:        req.GetApiCredential().GetOpenedAt(),
		},
	}, nil
}

// ListApiCredentialVersions lists api credentials a page at a time.
func (s *ApiCredentialHistoryServer) ListApiCredentialVersions(ctx context.Context, req *identityv1.ListApiCredentialVersionsRequest) (*identityv1.ListApiCredentialVersionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListApiCredentialVersions", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list api credential versions", "tenant", req.GetContext().GetTenantId())
	return &identityv1.ListApiCredentialVersionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// GetApiCredentialVersion returns an api credential.
func (s *ApiCredentialHistoryServer) GetApiCredentialVersion(ctx context.Context, req *identityv1.GetApiCredentialVersionRequest) (*identityv1.GetApiCredentialVersionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetApiCredentialVersion", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "api_credential", req.GetApiCredentialId(), "get_api_credential_version"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get api credential version", "tenant", req.GetContext().GetTenantId())
	return &identityv1.GetApiCredentialVersionResponse{
		ApiCredential: &identityv1.ApiCredential{ApiCredentialId: req.GetApiCredentialId()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CompareApiCredentialVersions compares an api credential.
func (s *ApiCredentialHistoryServer) CompareApiCredentialVersions(ctx context.Context, req *identityv1.CompareApiCredentialVersionsRequest) (*identityv1.CompareApiCredentialVersionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveApiCredential(ctx, req.GetContext().GetTenantId(), req.GetApiCredential()); err != nil {
		return nil, grpcx.Wrap(err, "save api credential")
	}
	s.log.InfoContext(ctx, "compare api credential versions", "tenant", req.GetContext().GetTenantId(), "etag", req.GetApiCredential().GetAudit().GetEtag())
	return &identityv1.CompareApiCredentialVersionsResponse{
		ApiCredential: &identityv1.ApiCredential{
			ApiCredentialId: req.GetApiCredentialId(),
			CurrencyCode:    req.GetApiCredential().GetCurrencyCode(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// AnnotateApiCredentialVersion annotates an api credential.
func (s *ApiCredentialHistoryServer) AnnotateApiCredentialVersion(ctx context.Context, req *identityv1.AnnotateApiCredentialVersionRequest) (*identityv1.AnnotateApiCredentialVersionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveApiCredential(ctx, req.GetContext().GetTenantId(), req.GetApiCredential()); err != nil {
		return nil, grpcx.Wrap(err, "save api credential")
	}
	s.log.InfoContext(ctx, "annotate api credential version", "tenant", req.GetContext().GetTenantId(), "etag", req.GetApiCredential().GetAudit().GetEtag())
	return &identityv1.AnnotateApiCredentialVersionResponse{
		ApiCredential: &identityv1.ApiCredential{
			ApiCredentialId: req.GetApiCredentialId(),
			Status:          req.GetApiCredential().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PruneApiCredentialVersions deletes an api credential.
func (s *ApiCredentialHistoryServer) PruneApiCredentialVersions(ctx context.Context, req *identityv1.PruneApiCredentialVersionsRequest) (*identityv1.PruneApiCredentialVersionsResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "api_credential", req.GetApiCredentialId(), "prune_api_credential_versions"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "prune api credential versions", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "api_credential_id", req.GetApiCredentialId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	sendApiCredentialTestNoticeRequest := &identityv1.SendApiCredentialTestNoticeRequest{
		Context:         req.GetContext(),
		ApiCredentialId: tenancy.Value(ctx, "api_credential_id"),
		Reason:          req.GetReason(),
		Details: &identityv1.SendApiCredentialTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
			Variables: baggage,
		},
	}
	sendApiCredentialTestNoticeReply, err := s.meteredApiCredentialNotification.SendApiCredentialTestNotice(ctx, sendApiCredentialTestNoticeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/SendApiCredentialTestNotice")
	}
	return &identityv1.PruneApiCredentialVersionsResponse{
		Result: &commonv1.OperationResult{
			Message:      sendApiCredentialTestNoticeReply.GetResult().GetMessage(),
			ReferenceIds: sendApiCredentialTestNoticeReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}
