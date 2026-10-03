package identity

import (
	"context"
	"database/sql"

	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
)

// Store keeps the records of the domain in its tables.
type Store struct {
	db *sql.DB
}

// NewStore returns a store on a database pool.
func NewStore(db *sql.DB) *Store { return &Store{db: db} }

// RecordAction appends to the audit table that an action ran on a record.
func (st *Store) RecordAction(ctx context.Context, tenant, kind, id, action string) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO actions (tenant_id, kind, record_id, action, at) VALUES ($1, $2, $3, $4, now())`,
		tenant, kind, id, action)
	return err
}

// SaveUser writes a user, replacing the stored one.
func (st *Store) SaveUser(ctx context.Context, tenant string, record *identityv1.User) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO users (tenant_id, user_id, status, etag) VALUES ($1, $2, $3, $4)
ON CONFLICT (tenant_id, user_id) DO UPDATE SET status = EXCLUDED.status`,
		tenant, record.GetUserId(), record.GetStatus(), record.GetAudit().GetEtag())
	return err
}

// SaveSession writes a session, replacing the stored one.
func (st *Store) SaveSession(ctx context.Context, tenant string, record *identityv1.Session) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO sessions (tenant_id, session_id, subject, status, priority, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, session_id) DO UPDATE SET subject = EXCLUDED.subject, status = EXCLUDED.status, priority = EXCLUDED.priority`,
		tenant, record.GetSessionId(), record.GetSubject(), record.GetStatus(), record.GetPriority(), record.GetAudit().GetEtag())
	return err
}

// SaveApiCredential writes a api credential, replacing the stored one.
func (st *Store) SaveApiCredential(ctx context.Context, tenant string, record *identityv1.ApiCredential) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO api_credentials (tenant_id, api_credential_id, status, currency_code, etag) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (tenant_id, api_credential_id) DO UPDATE SET status = EXCLUDED.status, currency_code = EXCLUDED.currency_code`,
		tenant, record.GetApiCredentialId(), record.GetStatus(), record.GetCurrencyCode(), record.GetAudit().GetEtag())
	return err
}

// SaveRole writes a role, replacing the stored one.
func (st *Store) SaveRole(ctx context.Context, tenant string, record *identityv1.Role) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO roles (tenant_id, role_id, name, description, status, priority, version, etag) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (tenant_id, role_id) DO UPDATE SET name = EXCLUDED.name, description = EXCLUDED.description, status = EXCLUDED.status, priority = EXCLUDED.priority, version = EXCLUDED.version`,
		tenant, record.GetRoleId(), record.GetName(), record.GetDescription(), record.GetStatus(), record.GetPriority(), record.GetVersion(), record.GetAudit().GetEtag())
	return err
}

// SaveTenant writes a tenant, replacing the stored one.
func (st *Store) SaveTenant(ctx context.Context, tenant string, record *identityv1.Tenant) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO tenants (tenant_id, tenant_id, name, parent_id, depth, status, etag) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (tenant_id, tenant_id) DO UPDATE SET name = EXCLUDED.name, parent_id = EXCLUDED.parent_id, depth = EXCLUDED.depth, status = EXCLUDED.status`,
		tenant, record.GetTenantId(), record.GetName(), record.GetParentId(), record.GetDepth(), record.GetStatus(), record.GetAudit().GetEtag())
	return err
}
