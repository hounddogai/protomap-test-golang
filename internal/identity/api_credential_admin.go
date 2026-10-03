package identity

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// ApiCredentialAdminServer implements `acme.identity.v1.ApiCredentialAdminService`.
// Operator actions on api credentials that support staff run by hand.
type ApiCredentialAdminServer struct {
	identityv1.UnimplementedApiCredentialAdminServiceServer
	store                  *Store
	log                    *slog.Logger
	searchQueryQueryClient searchv1.SearchQueryQueryServiceClient
	ledgerAccountPort      ledgerAccountPort
}

// NewApiCredentialAdminServer wires an ApiCredentialAdminServer.
func NewApiCredentialAdminServer(deps platform.Deps) *ApiCredentialAdminServer {
	return &ApiCredentialAdminServer{
		store:                  NewStore(deps.DB),
		log:                    deps.Logger.With("service", "acme.identity.v1.ApiCredentialAdminService"),
		searchQueryQueryClient: searchv1.NewSearchQueryQueryServiceClient(deps.Conn),
		ledgerAccountPort:      newLedgerAccountPort(deps),
	}
}

// ArchiveApiCredential archives an api credential.
func (s *ApiCredentialAdminServer) ArchiveApiCredential(ctx context.Context, req *identityv1.ArchiveApiCredentialRequest) (*identityv1.ArchiveApiCredentialResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ArchiveApiCredential", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "api_credential", req.GetApiCredentialId(), "archive_api_credential"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "archive api credential", "tenant", req.GetContext().GetTenantId())
	return &identityv1.ArchiveApiCredentialResponse{
		ApiCredential: &identityv1.ApiCredential{ApiCredentialId: req.GetApiCredentialId()},
		Outcome:       &identityv1.ArchiveApiCredentialResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RestoreApiCredential restores an api credential.
func (s *ApiCredentialAdminServer) RestoreApiCredential(ctx context.Context, req *identityv1.RestoreApiCredentialRequest) (*identityv1.RestoreApiCredentialResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "api_credential", req.GetApiCredentialId(), "restore_api_credential"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "restore api credential", "tenant", req.GetContext().GetTenantId())
	return &identityv1.RestoreApiCredentialResponse{
		ApiCredential: &identityv1.ApiCredential{ApiCredentialId: req.GetApiCredentialId()},
		Outcome:       &identityv1.RestoreApiCredentialResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PurgeApiCredential deletes an api credential.
func (s *ApiCredentialAdminServer) PurgeApiCredential(ctx context.Context, req *identityv1.PurgeApiCredentialRequest) (*identityv1.PurgeApiCredentialResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "api_credential", req.GetApiCredentialId(), "purge_api_credential"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "purge api credential", "tenant", req.GetContext().GetTenantId())
	return &identityv1.PurgeApiCredentialResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ReassignApiCredential reassigns an api credential.
func (s *ApiCredentialAdminServer) ReassignApiCredential(ctx context.Context, req *identityv1.ReassignApiCredentialRequest) (*identityv1.ReassignApiCredentialResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ReassignApiCredential", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "api_credential", req.GetApiCredentialId(), "reassign_api_credential"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reassign api credential", "tenant", req.GetContext().GetTenantId())
	return &identityv1.ReassignApiCredentialResponse{
		ApiCredential: &identityv1.ApiCredential{
			ApiCredentialId: req.GetApiCredentialId(),
			Contact:         &commonv1.ContactInfo{Email: req.GetDetails().GetAssignee().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LockApiCredential locks an api credential.
func (s *ApiCredentialAdminServer) LockApiCredential(ctx context.Context, req *identityv1.LockApiCredentialRequest) (*identityv1.LockApiCredentialResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "api_credential", req.GetApiCredentialId(), "lock_api_credential"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lock api credential", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded identityv1.LockApiCredentialRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	exportSearchQueriesStream, err := s.searchQueryQueryClient.ExportSearchQueries(ctx, &searchv1.ExportSearchQueriesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ExportSearchQueriesRequest_Filter{
			OwnerId: (&decoded).GetNewOwner().GetUserId(),
			Labels:  req.GetAnnotations(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/ExportSearchQueries")
	}
	for {
		received, err := exportSearchQueriesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/ExportSearchQueries")
		}
		_ = received
	}
	return &identityv1.LockApiCredentialResponse{
		ApiCredential: &identityv1.ApiCredential{ApiCredentialId: req.GetApiCredentialId()},
		Outcome:       &identityv1.LockApiCredentialResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// UnlockApiCredential unlocks an api credential.
func (s *ApiCredentialAdminServer) UnlockApiCredential(ctx context.Context, req *identityv1.UnlockApiCredentialRequest) (*identityv1.UnlockApiCredentialResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "api_credential", req.GetApiCredentialId(), "unlock_api_credential"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "unlock api credential", "tenant", req.GetContext().GetTenantId())
	return &identityv1.UnlockApiCredentialResponse{
		ApiCredential: &identityv1.ApiCredential{ApiCredentialId: req.GetApiCredentialId()},
		Outcome:       &identityv1.UnlockApiCredentialResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// TransferApiCredential transfers an api credential.
func (s *ApiCredentialAdminServer) TransferApiCredential(ctx context.Context, req *identityv1.TransferApiCredentialRequest) (*identityv1.TransferApiCredentialResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "api_credential", req.GetApiCredentialId(), "transfer_api_credential"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "transfer api credential", "tenant", req.GetContext().GetTenantId())
	return &identityv1.TransferApiCredentialResponse{
		ApiCredential: &identityv1.ApiCredential{ApiCredentialId: req.GetApiCredentialId()},
		Outcome:       &identityv1.TransferApiCredentialResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RepairApiCredential repairs an api credential.
func (s *ApiCredentialAdminServer) RepairApiCredential(ctx context.Context, req *identityv1.RepairApiCredentialRequest) (*identityv1.RepairApiCredentialResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RepairApiCredential", "error", err)
		return nil, err
	}
	if err := s.store.SaveApiCredential(ctx, req.GetContext().GetTenantId(), req.GetApiCredential()); err != nil {
		return nil, grpcx.Wrap(err, "save api credential")
	}
	s.log.InfoContext(ctx, "repair api credential", "tenant", req.GetContext().GetTenantId(), "etag", req.GetApiCredential().GetAudit().GetEtag())
	return &identityv1.RepairApiCredentialResponse{
		ApiCredential: &identityv1.ApiCredential{
			ApiCredentialId: req.GetApiCredentialId(),
			Contact:         req.GetApiCredential().GetContact(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// InspectApiCredential returns an api credential.
func (s *ApiCredentialAdminServer) InspectApiCredential(ctx context.Context, req *identityv1.InspectApiCredentialRequest) (*identityv1.InspectApiCredentialResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "api_credential", req.GetApiCredentialId(), "inspect_api_credential"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect api credential", "tenant", req.GetContext().GetTenantId())
	searchLedgerAccountsRequest := &ledgerv1.SearchLedgerAccountsRequest{
		Context: req.GetContext(),
		Filter: &ledgerv1.SearchLedgerAccountsRequest_Filter{
			OwnerId:      req.GetApiCredentialId(),
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	}
	searchLedgerAccountsReply, err := s.ledgerAccountPort.SearchLedgerAccounts(ctx, searchLedgerAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/SearchLedgerAccounts")
	}
	return &identityv1.InspectApiCredentialResponse{
		Result: &commonv1.OperationResult{
			Details: searchLedgerAccountsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		ApiCredential: &identityv1.ApiCredential{ApiCredentialId: req.GetApiCredentialId()},
	}, nil
}
