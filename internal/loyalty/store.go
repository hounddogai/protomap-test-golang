package loyalty

import (
	"context"
	"database/sql"

	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
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

// SaveLoyaltyMember writes a loyalty member, replacing the stored one.
func (st *Store) SaveLoyaltyMember(ctx context.Context, tenant string, record *loyaltyv1.LoyaltyMember) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO loyalty_members (tenant_id, loyalty_member_id, status, currency_code, etag) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (tenant_id, loyalty_member_id) DO UPDATE SET status = EXCLUDED.status, currency_code = EXCLUDED.currency_code`,
		tenant, record.GetLoyaltyMemberId(), record.GetStatus(), record.GetCurrencyCode(), record.GetAudit().GetEtag())
	return err
}

// SavePointsLedger writes a points ledger, replacing the stored one.
func (st *Store) SavePointsLedger(ctx context.Context, tenant string, record *loyaltyv1.PointsLedger) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO points_ledgers (tenant_id, points_ledger_id, status, currency_code, etag) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (tenant_id, points_ledger_id) DO UPDATE SET status = EXCLUDED.status, currency_code = EXCLUDED.currency_code`,
		tenant, record.GetPointsLedgerId(), record.GetStatus(), record.GetCurrencyCode(), record.GetAudit().GetEtag())
	return err
}

// SaveReward writes a reward, replacing the stored one.
func (st *Store) SaveReward(ctx context.Context, tenant string, record *loyaltyv1.Reward) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO rewards (tenant_id, reward_id, name, description, status, priority, version, etag) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (tenant_id, reward_id) DO UPDATE SET name = EXCLUDED.name, description = EXCLUDED.description, status = EXCLUDED.status, priority = EXCLUDED.priority, version = EXCLUDED.version`,
		tenant, record.GetRewardId(), record.GetName(), record.GetDescription(), record.GetStatus(), record.GetPriority(), record.GetVersion(), record.GetAudit().GetEtag())
	return err
}

// SaveTier writes a tier, replacing the stored one.
func (st *Store) SaveTier(ctx context.Context, tenant string, record *loyaltyv1.Tier) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO tiers (tenant_id, tier_id, name, description, status, priority, version, etag) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (tenant_id, tier_id) DO UPDATE SET name = EXCLUDED.name, description = EXCLUDED.description, status = EXCLUDED.status, priority = EXCLUDED.priority, version = EXCLUDED.version`,
		tenant, record.GetTierId(), record.GetName(), record.GetDescription(), record.GetStatus(), record.GetPriority(), record.GetVersion(), record.GetAudit().GetEtag())
	return err
}
