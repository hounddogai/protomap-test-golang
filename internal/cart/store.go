package cart

import (
	"context"
	"database/sql"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
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

// SaveCart writes a cart, replacing the stored one.
func (st *Store) SaveCart(ctx context.Context, tenant string, record *cartv1.Cart) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO carts (tenant_id, cart_id, number, status, notes, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, cart_id) DO UPDATE SET number = EXCLUDED.number, status = EXCLUDED.status, notes = EXCLUDED.notes`,
		tenant, record.GetCartId(), record.GetNumber(), record.GetStatus(), record.GetNotes(), record.GetAudit().GetEtag())
	return err
}

// SaveSavedList writes a saved list, replacing the stored one.
func (st *Store) SaveSavedList(ctx context.Context, tenant string, record *cartv1.SavedList) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO saved_lists (tenant_id, saved_list_id, number, status, notes, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, saved_list_id) DO UPDATE SET number = EXCLUDED.number, status = EXCLUDED.status, notes = EXCLUDED.notes`,
		tenant, record.GetSavedListId(), record.GetNumber(), record.GetStatus(), record.GetNotes(), record.GetAudit().GetEtag())
	return err
}

// SaveCartRule writes a cart rule, replacing the stored one.
func (st *Store) SaveCartRule(ctx context.Context, tenant string, record *cartv1.CartRule) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO cart_rules (tenant_id, cart_rule_id, name, description, status, priority, version, etag) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (tenant_id, cart_rule_id) DO UPDATE SET name = EXCLUDED.name, description = EXCLUDED.description, status = EXCLUDED.status, priority = EXCLUDED.priority, version = EXCLUDED.version`,
		tenant, record.GetCartRuleId(), record.GetName(), record.GetDescription(), record.GetStatus(), record.GetPriority(), record.GetVersion(), record.GetAudit().GetEtag())
	return err
}
