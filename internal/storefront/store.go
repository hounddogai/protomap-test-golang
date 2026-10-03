package storefront

import (
	"context"
	"database/sql"

	storefrontv1 "github.com/hounddogai/protomap-test-golang/gen/acme/storefront/v1"
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

// SaveStorefrontSession writes a storefront session, replacing the stored one.
func (st *Store) SaveStorefrontSession(ctx context.Context, tenant string, record *storefrontv1.StorefrontSession) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO storefront_sessions (tenant_id, storefront_session_id, subject, status, priority, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, storefront_session_id) DO UPDATE SET subject = EXCLUDED.subject, status = EXCLUDED.status, priority = EXCLUDED.priority`,
		tenant, record.GetStorefrontSessionId(), record.GetSubject(), record.GetStatus(), record.GetPriority(), record.GetAudit().GetEtag())
	return err
}

// SaveShopperView writes a shopper view, replacing the stored one.
func (st *Store) SaveShopperView(ctx context.Context, tenant string, record *storefrontv1.ShopperView) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO shopper_views (tenant_id, shopper_view_id, code, name, status, capacity, etag) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (tenant_id, shopper_view_id) DO UPDATE SET code = EXCLUDED.code, name = EXCLUDED.name, status = EXCLUDED.status, capacity = EXCLUDED.capacity`,
		tenant, record.GetShopperViewId(), record.GetCode(), record.GetName(), record.GetStatus(), record.GetCapacity(), record.GetAudit().GetEtag())
	return err
}

// SaveLandingPage writes a landing page, replacing the stored one.
func (st *Store) SaveLandingPage(ctx context.Context, tenant string, record *storefrontv1.LandingPage) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO landing_pages (tenant_id, landing_page_id, number, status, notes, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, landing_page_id) DO UPDATE SET number = EXCLUDED.number, status = EXCLUDED.status, notes = EXCLUDED.notes`,
		tenant, record.GetLandingPageId(), record.GetNumber(), record.GetStatus(), record.GetNotes(), record.GetAudit().GetEtag())
	return err
}
