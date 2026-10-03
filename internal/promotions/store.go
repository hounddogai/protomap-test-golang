package promotions

import (
	"context"
	"database/sql"

	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
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

// SavePromotion writes a promotion, replacing the stored one.
func (st *Store) SavePromotion(ctx context.Context, tenant string, record *promotionsv1.Promotion) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO promotions (tenant_id, promotion_id, name, description, status, priority, version, etag) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (tenant_id, promotion_id) DO UPDATE SET name = EXCLUDED.name, description = EXCLUDED.description, status = EXCLUDED.status, priority = EXCLUDED.priority, version = EXCLUDED.version`,
		tenant, record.GetPromotionId(), record.GetName(), record.GetDescription(), record.GetStatus(), record.GetPriority(), record.GetVersion(), record.GetAudit().GetEtag())
	return err
}

// SaveCoupon writes a coupon, replacing the stored one.
func (st *Store) SaveCoupon(ctx context.Context, tenant string, record *promotionsv1.Coupon) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO coupons (tenant_id, coupon_id, status, currency_code, etag) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (tenant_id, coupon_id) DO UPDATE SET status = EXCLUDED.status, currency_code = EXCLUDED.currency_code`,
		tenant, record.GetCouponId(), record.GetStatus(), record.GetCurrencyCode(), record.GetAudit().GetEtag())
	return err
}

// SaveCampaign writes a campaign, replacing the stored one.
func (st *Store) SaveCampaign(ctx context.Context, tenant string, record *promotionsv1.Campaign) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO campaigns (tenant_id, campaign_id, title, status, etag) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (tenant_id, campaign_id) DO UPDATE SET title = EXCLUDED.title, status = EXCLUDED.status`,
		tenant, record.GetCampaignId(), record.GetTitle(), record.GetStatus(), record.GetAudit().GetEtag())
	return err
}

// SaveRedemption writes a redemption, replacing the stored one.
func (st *Store) SaveRedemption(ctx context.Context, tenant string, record *promotionsv1.Redemption) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO redemptions (tenant_id, redemption_id, kind, source, correlation_id, sequence) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, redemption_id) DO UPDATE SET kind = EXCLUDED.kind, source = EXCLUDED.source, correlation_id = EXCLUDED.correlation_id, sequence = EXCLUDED.sequence`,
		tenant, record.GetRedemptionId(), record.GetKind(), record.GetSource(), record.GetCorrelationId(), record.GetSequence())
	return err
}
