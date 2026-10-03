package identity

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TenantExportServer implements `acme.identity.v1.TenantExportService`.
// Exports tenants to files and other systems.
type TenantExportServer struct {
	identityv1.UnimplementedTenantExportServiceServer
	store                     *Store
	log                       *slog.Logger
	sessionNotificationClient identityv1.SessionNotificationServiceClient
}

// NewTenantExportServer wires a TenantExportServer.
func NewTenantExportServer(deps platform.Deps) *TenantExportServer {
	return &TenantExportServer{
		store:                     NewStore(deps.DB),
		log:                       deps.Logger.With("service", "acme.identity.v1.TenantExportService"),
		sessionNotificationClient: identityv1.NewSessionNotificationServiceClient(deps.Conn),
	}
}

// StreamTenantExport streams every matching tenant.
func (s *TenantExportServer) StreamTenantExport(req *identityv1.StreamTenantExportRequest, stream grpc.ServerStreamingServer[identityv1.StreamTenantExportResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		return err
	}
	s.log.InfoContext(ctx, "stream tenant export", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	streamSessionNoticesStream, err := s.sessionNotificationClient.StreamSessionNotices(ctx, &identityv1.StreamSessionNoticesRequest{
		Context: req.GetContext(),
		Filter: &identityv1.StreamSessionNoticesRequest_Filter{
			Query:  req.GetFilter().GetQuery(),
			Labels: labels,
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/StreamSessionNotices")
	}
	for {
		received, err := streamSessionNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/StreamSessionNotices")
		}
		_ = received
	}
	if err := stream.Send(&identityv1.StreamTenantExportResponse{
		Tenant: &identityv1.Tenant{
			Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}); err != nil {
		return err
	}
	return nil
}

// GetTenantExport returns a tenant.
func (s *TenantExportServer) GetTenantExport(ctx context.Context, req *identityv1.GetTenantExportRequest) (*identityv1.GetTenantExportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tenant", req.GetTenantId(), "get_tenant_export"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get tenant export", "tenant", req.GetContext().GetTenantId())
	return &identityv1.GetTenantExportResponse{
		Tenant: &identityv1.Tenant{TenantId: req.GetTenantId()},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CancelTenantExport cancels a tenant.
func (s *TenantExportServer) CancelTenantExport(ctx context.Context, req *identityv1.CancelTenantExportRequest) (*identityv1.CancelTenantExportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tenant", req.GetTenantId(), "cancel_tenant_export"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "cancel tenant export", "tenant", req.GetContext().GetTenantId())
	return &identityv1.CancelTenantExportResponse{
		Tenant:  &identityv1.Tenant{TenantId: req.GetTenantId()},
		Outcome: &identityv1.CancelTenantExportResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListTenantExports lists tenants a page at a time.
func (s *TenantExportServer) ListTenantExports(ctx context.Context, req *identityv1.ListTenantExportsRequest) (*identityv1.ListTenantExportsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list tenant exports", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	sendSessionTestNoticeRequest := &identityv1.SendSessionTestNoticeRequest{
		Context: req.GetContext(),
		Details: &identityv1.SendSessionTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Variables: labels,
		},
	}
	sendSessionTestNoticeReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*identityv1.SendSessionTestNoticeResponse, error) {
		return s.sessionNotificationClient.SendSessionTestNotice(ctx, sendSessionTestNoticeRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/SendSessionTestNotice")
	}
	return &identityv1.ListTenantExportsResponse{
		Result: &commonv1.OperationResult{
			Details: sendSessionTestNoticeReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Tenants: []*identityv1.Tenant{&identityv1.Tenant{
			Children: []*identityv1.Tenant{&identityv1.Tenant{
				Manager: &identityv1.UserRef{
					Email: sendSessionTestNoticeReply.GetSession().GetRequester().GetEmail(),
				},
			}},
		}},
	}, nil
}

// ScheduleTenantExport schedules a tenant.
func (s *TenantExportServer) ScheduleTenantExport(ctx context.Context, req *identityv1.ScheduleTenantExportRequest) (*identityv1.ScheduleTenantExportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tenant", req.GetTenantId(), "schedule_tenant_export"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule tenant export", "tenant", req.GetContext().GetTenantId())
	return &identityv1.ScheduleTenantExportResponse{
		Period: req.GetDetails().GetPeriod(),
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RetryTenantExport retrys a tenant.
func (s *TenantExportServer) RetryTenantExport(ctx context.Context, req *identityv1.RetryTenantExportRequest) (*identityv1.RetryTenantExportResponse, error) {
	if err := s.store.SaveTenant(ctx, req.GetContext().GetTenantId(), req.GetTenant()); err != nil {
		return nil, grpcx.Wrap(err, "save tenant")
	}
	s.log.InfoContext(ctx, "retry tenant export", "tenant", req.GetContext().GetTenantId(), "etag", req.GetTenant().GetAudit().GetEtag())
	return &identityv1.RetryTenantExportResponse{
		Tenant: &identityv1.Tenant{
			TenantId: req.GetTenantId(),
			Location: req.GetTenant().GetLocation(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
