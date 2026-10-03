package search

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SynonymAdminServer implements `acme.search.v1.SynonymAdminService`.
// Operator actions on synonyms that support staff run by hand.
type SynonymAdminServer struct {
	searchv1.UnsafeSynonymAdminServiceServer
	store               *Store
	log                 *slog.Logger
	conn                *grpc.ClientConn
	synonymExportClient searchv1.SynonymExportServiceClient
	clients             *registry.Registry
}

// NewSynonymAdminServer wires a SynonymAdminServer.
func NewSynonymAdminServer(deps platform.Deps) *SynonymAdminServer {
	return &SynonymAdminServer{
		store:               NewStore(deps.DB),
		log:                 deps.Logger.With("service", "acme.search.v1.SynonymAdminService"),
		conn:                deps.Conn,
		synonymExportClient: searchv1.NewSynonymExportServiceClient(deps.Conn),
		clients:             deps.Clients,
	}
}

// ArchiveSynonym archives a synonym.
func (s *SynonymAdminServer) ArchiveSynonym(ctx context.Context, req *searchv1.ArchiveSynonymRequest) (*searchv1.ArchiveSynonymResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ArchiveSynonym", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "synonym", req.GetSynonymId(), "archive_synonym"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "archive synonym", "tenant", req.GetContext().GetTenantId())
	getSynonymSyncStateRequest := &searchv1.GetSynonymSyncStateRequest{
		Context:   req.GetContext(),
		SynonymId: req.GetSynonymId(),
	}
	getSynonymSyncStateReply := new(searchv1.GetSynonymSyncStateResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.search.v1.SynonymSyncService/GetSynonymSyncState", getSynonymSyncStateRequest, getSynonymSyncStateReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/GetSynonymSyncState")
	}
	return &searchv1.ArchiveSynonymResponse{
		Outcome: &searchv1.ArchiveSynonymResponse_Outcome{
			Message:     getSynonymSyncStateReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Synonym: &searchv1.Synonym{
			Name:      getSynonymSyncStateReply.GetSynonym().GetName(),
			SynonymId: req.GetSynonymId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RestoreSynonym restores a synonym.
func (s *SynonymAdminServer) RestoreSynonym(ctx context.Context, req *searchv1.RestoreSynonymRequest) (*searchv1.RestoreSynonymResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RestoreSynonym", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "synonym", req.GetSynonymId(), "restore_synonym"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "restore synonym", "tenant", req.GetContext().GetTenantId())
	return &searchv1.RestoreSynonymResponse{
		Synonym: &searchv1.Synonym{SynonymId: req.GetSynonymId()},
		Outcome: &searchv1.RestoreSynonymResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PurgeSynonym deletes a synonym.
func (s *SynonymAdminServer) PurgeSynonym(ctx context.Context, req *searchv1.PurgeSynonymRequest) (*searchv1.PurgeSynonymResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "synonym", req.GetSynonymId(), "purge_synonym"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "purge synonym", "tenant", req.GetContext().GetTenantId())
	return &searchv1.PurgeSynonymResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ReassignSynonym reassigns a synonym.
func (s *SynonymAdminServer) ReassignSynonym(ctx context.Context, req *searchv1.ReassignSynonymRequest) (*searchv1.ReassignSynonymResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "synonym", req.GetSynonymId(), "reassign_synonym"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reassign synonym", "tenant", req.GetContext().GetTenantId())
	return &searchv1.ReassignSynonymResponse{
		Synonym: &searchv1.Synonym{
			SynonymId: req.GetSynonymId(),
			Owner:     &identityv1.UserRef{UserId: req.GetDetails().GetAssignee().GetUserId()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LockSynonym locks a synonym.
func (s *SynonymAdminServer) LockSynonym(ctx context.Context, req *searchv1.LockSynonymRequest) (*searchv1.LockSynonymResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LockSynonym", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "synonym", req.GetSynonymId(), "lock_synonym"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lock synonym", "tenant", req.GetContext().GetTenantId())
	shareSynonymExportRequest := &searchv1.ShareSynonymExportRequest{}
	shareSynonymExportRequest.Context = req.GetContext()
	shareSynonymExportRequest.Details = &searchv1.ShareSynonymExportRequest_Details{
		Recipient: &commonv1.ContactInfo{Name: req.GetDetails().GetNotifyContact().GetName()},
	}
	shareSynonymExportReply, err := s.synonymExportClient.ShareSynonymExport(ctx, shareSynonymExportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/ShareSynonymExport")
	}
	return &searchv1.LockSynonymResponse{
		Synonym: &searchv1.Synonym{
			Effective: shareSynonymExportReply.GetSynonym().GetEffective(),
			SynonymId: req.GetSynonymId(),
		},
		Outcome: &searchv1.LockSynonymResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// UnlockSynonym unlocks a synonym.
func (s *SynonymAdminServer) UnlockSynonym(ctx context.Context, req *searchv1.UnlockSynonymRequest) (*searchv1.UnlockSynonymResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "synonym", req.GetSynonymId(), "unlock_synonym"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "unlock synonym", "tenant", req.GetContext().GetTenantId())
	return &searchv1.UnlockSynonymResponse{
		Synonym: &searchv1.Synonym{SynonymId: req.GetSynonymId()},
		Outcome: &searchv1.UnlockSynonymResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// TransferSynonym transfers a synonym.
func (s *SynonymAdminServer) TransferSynonym(ctx context.Context, req *searchv1.TransferSynonymRequest) (*searchv1.TransferSynonymResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "synonym", req.GetSynonymId(), "transfer_synonym"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "transfer synonym", "tenant", req.GetContext().GetTenantId())
	synonymSync, err := registry.Lookup[searchv1.SynonymSyncServiceClient](s.clients, "acme.search.v1.SynonymSyncService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resolveSynonymConflictRequest := &searchv1.ResolveSynonymConflictRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{
				Email:  req.GetDetails().GetNewOwner().GetEmail(),
				UserId: req.GetDetails().GetNewOwner().GetUserId(),
			},
			SynonymId: req.GetSynonymId(),
		},
	}
	resolveSynonymConflictReply, err := synonymSync.ResolveSynonymConflict(ctx, resolveSynonymConflictRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/ResolveSynonymConflict")
	}
	return &searchv1.TransferSynonymResponse{
		Outcome: &searchv1.TransferSynonymResponse_Outcome{
			Message:     resolveSynonymConflictReply.GetResult().GetMessage(),
			Status:      resolveSynonymConflictReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Synonym: &searchv1.Synonym{SynonymId: req.GetSynonymId()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RepairSynonym repairs a synonym.
func (s *SynonymAdminServer) RepairSynonym(ctx context.Context, req *searchv1.RepairSynonymRequest) (*searchv1.RepairSynonymResponse, error) {
	if err := s.store.SaveSynonym(ctx, req.GetContext().GetTenantId(), req.GetSynonym()); err != nil {
		return nil, grpcx.Wrap(err, "save synonym")
	}
	s.log.InfoContext(ctx, "repair synonym", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSynonym().GetAudit().GetEtag())
	return &searchv1.RepairSynonymResponse{
		Synonym: &searchv1.Synonym{
			SynonymId: req.GetSynonymId(),
			Priority:  req.GetSynonym().GetPriority(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// InspectSynonym returns a synonym.
func (s *SynonymAdminServer) InspectSynonym(ctx context.Context, req *searchv1.InspectSynonymRequest) (*searchv1.InspectSynonymResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "synonym", req.GetSynonymId(), "inspect_synonym"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect synonym", "tenant", req.GetContext().GetTenantId())
	return &searchv1.InspectSynonymResponse{
		Synonym: &searchv1.Synonym{SynonymId: req.GetSynonymId()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}
