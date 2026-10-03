package identity

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// RoleAdminServer implements `acme.identity.v1.RoleAdminService`.
// Operator actions on roles that support staff run by hand.
type RoleAdminServer struct {
	identityv1.UnimplementedRoleAdminServiceServer
	store              *Store
	log                *slog.Logger
	tenantExportClient identityv1.TenantExportServiceClient
	conn               *grpc.ClientConn
}

// NewRoleAdminServer wires a RoleAdminServer.
func NewRoleAdminServer(deps platform.Deps) *RoleAdminServer {
	return &RoleAdminServer{
		store:              NewStore(deps.DB),
		log:                deps.Logger.With("service", "acme.identity.v1.RoleAdminService"),
		tenantExportClient: identityv1.NewTenantExportServiceClient(deps.Conn),
		conn:               deps.Conn,
	}
}

// ArchiveRole archives a role.
func (s *RoleAdminServer) ArchiveRole(ctx context.Context, req *identityv1.ArchiveRoleRequest) (*identityv1.ArchiveRoleResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "role", req.GetRoleId(), "archive_role"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "archive role", "tenant", req.GetContext().GetTenantId())
	listTenantExportsRequest := &identityv1.ListTenantExportsRequest{}
	listTenantExportsRequest.Context = req.GetContext()
	listTenantExportsRequest.Filter = &identityv1.ListTenantExportsRequest_Filter{
		CreatedAfter: req.GetDetails().GetEffectiveAt(),
		OwnerId:      req.GetRoleId(),
	}
	listTenantExportsReply, err := s.tenantExportClient.ListTenantExports(ctx, listTenantExportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/ListTenantExports")
	}
	return &identityv1.ArchiveRoleResponse{
		Result:  &commonv1.OperationResult{Status: listTenantExportsReply.GetResult().GetStatus()},
		Role:    &identityv1.Role{RoleId: req.GetRoleId()},
		Outcome: &identityv1.ArchiveRoleResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// RestoreRole restores a role.
func (s *RoleAdminServer) RestoreRole(ctx context.Context, req *identityv1.RestoreRoleRequest) (*identityv1.RestoreRoleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "role", req.GetRoleId(), "restore_role"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "restore role", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	subscribeSessionUpdatesRequest := &identityv1.SubscribeSessionUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.SubscribeSessionUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Phone:            req.GetDetails().GetNotifyContact().GetPhone(),
				Email:            req.GetDetails().GetNewOwner().GetEmail(),
				Name:             req.GetDetails().GetNotifyContact().GetName(),
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			},
			Payload: packed,
		},
	}
	subscribeSessionUpdatesReply, err := grpcx.NewClient(s.conn, identityv1.NewSessionNotificationServiceClient).SubscribeSessionUpdates(ctx, subscribeSessionUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/SubscribeSessionUpdates")
	}
	return &identityv1.RestoreRoleResponse{
		Role: &identityv1.Role{
			Priority: subscribeSessionUpdatesReply.GetSession().GetPriority(),
			RoleId:   req.GetRoleId(),
		},
		Outcome: &identityv1.RestoreRoleResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PurgeRole deletes a role.
func (s *RoleAdminServer) PurgeRole(ctx context.Context, req *identityv1.PurgeRoleRequest) (*identityv1.PurgeRoleResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "role", req.GetRoleId(), "purge_role"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "purge role", "tenant", req.GetContext().GetTenantId())
	return &identityv1.PurgeRoleResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// LockRole locks a role.
func (s *RoleAdminServer) LockRole(ctx context.Context, req *identityv1.LockRoleRequest) (*identityv1.LockRoleResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "role", req.GetRoleId(), "lock_role"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lock role", "tenant", req.GetContext().GetTenantId())
	return &identityv1.LockRoleResponse{
		Role:    &identityv1.Role{RoleId: req.GetRoleId()},
		Outcome: &identityv1.LockRoleResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// UnlockRole unlocks a role.
func (s *RoleAdminServer) UnlockRole(ctx context.Context, req *identityv1.UnlockRoleRequest) (*identityv1.UnlockRoleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "role", req.GetRoleId(), "unlock_role"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "unlock role", "tenant", req.GetContext().GetTenantId())
	return &identityv1.UnlockRoleResponse{
		Role:    &identityv1.Role{RoleId: req.GetRoleId()},
		Outcome: &identityv1.UnlockRoleResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// TransferRole transfers a role.
func (s *RoleAdminServer) TransferRole(ctx context.Context, req *identityv1.TransferRoleRequest) (*identityv1.TransferRoleResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "role", req.GetRoleId(), "transfer_role"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "transfer role", "tenant", req.GetContext().GetTenantId())
	return &identityv1.TransferRoleResponse{
		Role:    &identityv1.Role{RoleId: req.GetRoleId()},
		Outcome: &identityv1.TransferRoleResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RepairRole repairs a role.
func (s *RoleAdminServer) RepairRole(ctx context.Context, req *identityv1.RepairRoleRequest) (*identityv1.RepairRoleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRole(ctx, req.GetContext().GetTenantId(), req.GetRole()); err != nil {
		return nil, grpcx.Wrap(err, "save role")
	}
	s.log.InfoContext(ctx, "repair role", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRole().GetAudit().GetEtag())
	return &identityv1.RepairRoleResponse{
		Role: &identityv1.Role{
			RoleId:  req.GetRoleId(),
			Version: req.GetRole().GetVersion(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// InspectRole returns a role.
func (s *RoleAdminServer) InspectRole(ctx context.Context, req *identityv1.InspectRoleRequest) (*identityv1.InspectRoleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected InspectRole", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "role", req.GetRoleId(), "inspect_role"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect role", "tenant", req.GetContext().GetTenantId())
	scheduleTenantExportRequest := &identityv1.ScheduleTenantExportRequest{
		Context:     req.GetContext(),
		TenantId:    req.GetContext().GetTenantId(),
		Annotations: req.GetContext().GetBaggage(),
	}
	type scheduleTenantExportResult struct {
		reply *identityv1.ScheduleTenantExportResponse
		err   error
	}
	scheduleTenantExportResults := make(chan scheduleTenantExportResult, 1)
	go func() {
		reply, err := s.tenantExportClient.ScheduleTenantExport(ctx, scheduleTenantExportRequest)
		scheduleTenantExportResults <- scheduleTenantExportResult{reply, err}
	}()
	var scheduleTenantExportReply *identityv1.ScheduleTenantExportResponse
	select {
	case result := <-scheduleTenantExportResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.identity.v1.TenantExportService/ScheduleTenantExport")
		}
		scheduleTenantExportReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &identityv1.InspectRoleResponse{
		Result: &commonv1.OperationResult{
			Details:      scheduleTenantExportReply.GetResult().GetDetails(),
			ReferenceIds: scheduleTenantExportReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Role: &identityv1.Role{RoleId: req.GetRoleId()},
	}, nil
}
