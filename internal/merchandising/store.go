package merchandising

import (
	"context"
	"database/sql"

	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
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

// SaveCollection writes a collection, replacing the stored one.
func (st *Store) SaveCollection(ctx context.Context, tenant string, record *merchandisingv1.Collection) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO collections (tenant_id, collection_id, name, parent_id, depth, status, etag) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (tenant_id, collection_id) DO UPDATE SET name = EXCLUDED.name, parent_id = EXCLUDED.parent_id, depth = EXCLUDED.depth, status = EXCLUDED.status`,
		tenant, record.GetCollectionId(), record.GetName(), record.GetParentId(), record.GetDepth(), record.GetStatus(), record.GetAudit().GetEtag())
	return err
}

// SavePlacement writes a placement, replacing the stored one.
func (st *Store) SavePlacement(ctx context.Context, tenant string, record *merchandisingv1.Placement) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO placements (tenant_id, placement_id, title, status, etag) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (tenant_id, placement_id) DO UPDATE SET title = EXCLUDED.title, status = EXCLUDED.status`,
		tenant, record.GetPlacementId(), record.GetTitle(), record.GetStatus(), record.GetAudit().GetEtag())
	return err
}

// SaveMerchandisingRule writes a merchandising rule, replacing the stored one.
func (st *Store) SaveMerchandisingRule(ctx context.Context, tenant string, record *merchandisingv1.MerchandisingRule) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO merchandising_rules (tenant_id, merchandising_rule_id, name, description, status, priority, version, etag) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (tenant_id, merchandising_rule_id) DO UPDATE SET name = EXCLUDED.name, description = EXCLUDED.description, status = EXCLUDED.status, priority = EXCLUDED.priority, version = EXCLUDED.version`,
		tenant, record.GetMerchandisingRuleId(), record.GetName(), record.GetDescription(), record.GetStatus(), record.GetPriority(), record.GetVersion(), record.GetAudit().GetEtag())
	return err
}
