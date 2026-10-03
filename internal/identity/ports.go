package identity

import (
	"context"
	"log/slog"

	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
)

// apiCredentialAdminPort is what the identity domain needs of acme.identity.v1.ApiCredentialAdminService.
type apiCredentialAdminPort interface {
	RepairApiCredential(ctx context.Context, request *identityv1.RepairApiCredentialRequest) (*identityv1.RepairApiCredentialResponse, error)
}

// grpcApiCredentialAdminPort reaches acme.identity.v1.ApiCredentialAdminService over gRPC.
type grpcApiCredentialAdminPort struct {
	client identityv1.ApiCredentialAdminServiceClient
}

// sandboxApiCredentialAdminPort answers in the sandbox environment, where acme.identity.v1.ApiCredentialAdminService is
// not deployed.
type sandboxApiCredentialAdminPort struct{ log *slog.Logger }

func (p grpcApiCredentialAdminPort) RepairApiCredential(ctx context.Context, request *identityv1.RepairApiCredentialRequest) (*identityv1.RepairApiCredentialResponse, error) {
	return p.client.RepairApiCredential(ctx, request)
}

func (p sandboxApiCredentialAdminPort) RepairApiCredential(ctx context.Context, request *identityv1.RepairApiCredentialRequest) (*identityv1.RepairApiCredentialResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers RepairApiCredential")
	return &identityv1.RepairApiCredentialResponse{}, nil
}

func newApiCredentialAdminPort(deps platform.Deps) apiCredentialAdminPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxApiCredentialAdminPort{log: deps.Logger}
	}
	return grpcApiCredentialAdminPort{client: identityv1.NewApiCredentialAdminServiceClient(deps.Conn)}
}

// ledgerAccountPort is what the identity domain needs of acme.ledger.v1.LedgerAccountService.
type ledgerAccountPort interface {
	SearchLedgerAccounts(ctx context.Context, request *ledgerv1.SearchLedgerAccountsRequest) (*ledgerv1.SearchLedgerAccountsResponse, error)
}

// grpcLedgerAccountPort reaches acme.ledger.v1.LedgerAccountService over gRPC.
type grpcLedgerAccountPort struct {
	client ledgerv1.LedgerAccountServiceClient
}

// sandboxLedgerAccountPort answers in the sandbox environment, where acme.ledger.v1.LedgerAccountService is not
// deployed.
type sandboxLedgerAccountPort struct{ log *slog.Logger }

func (p grpcLedgerAccountPort) SearchLedgerAccounts(ctx context.Context, request *ledgerv1.SearchLedgerAccountsRequest) (*ledgerv1.SearchLedgerAccountsResponse, error) {
	return p.client.SearchLedgerAccounts(ctx, request)
}

func (p sandboxLedgerAccountPort) SearchLedgerAccounts(ctx context.Context, request *ledgerv1.SearchLedgerAccountsRequest) (*ledgerv1.SearchLedgerAccountsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SearchLedgerAccounts")
	return &ledgerv1.SearchLedgerAccountsResponse{}, nil
}

func newLedgerAccountPort(deps platform.Deps) ledgerAccountPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxLedgerAccountPort{log: deps.Logger}
	}
	return grpcLedgerAccountPort{client: ledgerv1.NewLedgerAccountServiceClient(deps.Conn)}
}
