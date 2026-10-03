package identity

import (
	"context"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// TenantAdminServer implements `acme.identity.v1.TenantAdminService`.
// Operator actions on tenants that support staff run by hand.
type TenantAdminServer struct {
	identityv1.UnimplementedTenantAdminServiceServer
	store                      *Store
	log                        *slog.Logger
	tenantExportClient         identityv1.TenantExportServiceClient
	auditEventQueryClient      auditv1.AuditEventQueryServiceClient
	auditEventValidationClient auditv1.AuditEventValidationServiceClient
}

// NewTenantAdminServer wires a TenantAdminServer.
func NewTenantAdminServer(deps platform.Deps) *TenantAdminServer {
	return &TenantAdminServer{
		store:                      NewStore(deps.DB),
		log:                        deps.Logger.With("service", "acme.identity.v1.TenantAdminService"),
		tenantExportClient:         identityv1.NewTenantExportServiceClient(deps.Conn),
		auditEventQueryClient:      auditv1.NewAuditEventQueryServiceClient(deps.Conn),
		auditEventValidationClient: auditv1.NewAuditEventValidationServiceClient(deps.Conn),
	}
}

// ArchiveTenant archives a tenant.
func (s *TenantAdminServer) ArchiveTenant(ctx context.Context, req *identityv1.ArchiveTenantRequest) (*identityv1.ArchiveTenantResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ArchiveTenant", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tenant", req.GetTenantId(), "archive_tenant"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "archive tenant", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "tenant_id", req.GetTenantId())
	cancelTenantExportRequest := &identityv1.CancelTenantExportRequest{
		Context:  req.GetContext(),
		TenantId: tenancy.Value(ctx, "tenant_id"),
		Details: &identityv1.CancelTenantExportRequest_Details{
			NotifyContact: &commonv1.ContactInfo{
				Email: req.GetDetails().GetNewOwner().GetEmail(),
				Name:  req.GetDetails().GetNotifyContact().GetName(),
			},
		},
	}
	cancelTenantExportReply, err := grpcx.Call(ctx, s.tenantExportClient.CancelTenantExport, cancelTenantExportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/CancelTenantExport")
	}
	return &identityv1.ArchiveTenantResponse{
		Outcome: &identityv1.ArchiveTenantResponse_Outcome{
			Status:      cancelTenantExportReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: cancelTenantExportReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Tenant: &identityv1.Tenant{TenantId: req.GetTenantId()},
	}, nil
}

// RestoreTenant restores a tenant.
func (s *TenantAdminServer) RestoreTenant(ctx context.Context, req *identityv1.RestoreTenantRequest) (*identityv1.RestoreTenantResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tenant", req.GetTenantId(), "restore_tenant"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "restore tenant", "tenant", req.GetContext().GetTenantId())
	findAuditEventsReply, err := s.auditEventQueryClient.FindAuditEvents(ctx, &auditv1.FindAuditEventsRequest{
		Context: req.GetContext(),
		Filter: &auditv1.FindAuditEventsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventQueryService/FindAuditEvents")
	}
	return &identityv1.RestoreTenantResponse{
		Result:  &commonv1.OperationResult{Status: findAuditEventsReply.GetResult().GetStatus()},
		Tenant:  &identityv1.Tenant{TenantId: req.GetTenantId()},
		Outcome: &identityv1.RestoreTenantResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// PurgeTenant deletes a tenant.
func (s *TenantAdminServer) PurgeTenant(ctx context.Context, req *identityv1.PurgeTenantRequest) (*identityv1.PurgeTenantResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PurgeTenant", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tenant", req.GetTenantId(), "purge_tenant"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "purge tenant", "tenant", req.GetContext().GetTenantId())
	return &identityv1.PurgeTenantResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ReassignTenant reassigns a tenant.
func (s *TenantAdminServer) ReassignTenant(ctx context.Context, req *identityv1.ReassignTenantRequest) (*identityv1.ReassignTenantResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tenant", req.GetTenantId(), "reassign_tenant"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reassign tenant", "tenant", req.GetContext().GetTenantId())
	return &identityv1.ReassignTenantResponse{
		Tenant: &identityv1.Tenant{
			TenantId: req.GetTenantId(),
			Manager: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetAssignee().GetDisplayName(),
			},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LockTenant locks a tenant.
func (s *TenantAdminServer) LockTenant(ctx context.Context, req *identityv1.LockTenantRequest) (*identityv1.LockTenantResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LockTenant", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tenant", req.GetTenantId(), "lock_tenant"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lock tenant", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNewOwner().GetEmail())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	lintAuditEventsReply, err := s.auditEventValidationClient.LintAuditEvents(ctx, &auditv1.LintAuditEventsRequest{
		Context: req.GetContext(),
		Details: &auditv1.LintAuditEventsRequest_Details{
			Contact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		},
		AuditEvent: &auditv1.AuditEvent{Detail: packed},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventValidationService/LintAuditEvents")
	}
	return &identityv1.LockTenantResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: lintAuditEventsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Tenant:  &identityv1.Tenant{TenantId: req.GetTenantId()},
		Outcome: &identityv1.LockTenantResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// UnlockTenant unlocks a tenant.
func (s *TenantAdminServer) UnlockTenant(ctx context.Context, req *identityv1.UnlockTenantRequest) (*identityv1.UnlockTenantResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected UnlockTenant", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tenant", req.GetTenantId(), "unlock_tenant"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "unlock tenant", "tenant", req.GetContext().GetTenantId())
	return &identityv1.UnlockTenantResponse{
		Tenant:  &identityv1.Tenant{TenantId: req.GetTenantId()},
		Outcome: &identityv1.UnlockTenantResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// InspectTenant returns a tenant.
func (s *TenantAdminServer) InspectTenant(ctx context.Context, req *identityv1.InspectTenantRequest) (*identityv1.InspectTenantResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tenant", req.GetTenantId(), "inspect_tenant"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect tenant", "tenant", req.GetContext().GetTenantId())
	return &identityv1.InspectTenantResponse{
		Tenant: &identityv1.Tenant{TenantId: req.GetTenantId()},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
