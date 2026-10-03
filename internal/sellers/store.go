package sellers

import (
	"context"
	"database/sql"

	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
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

// SaveSeller writes a seller, replacing the stored one.
func (st *Store) SaveSeller(ctx context.Context, tenant string, record *sellersv1.Seller) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO sellers (tenant_id, seller_id, status, etag) VALUES ($1, $2, $3, $4)
ON CONFLICT (tenant_id, seller_id) DO UPDATE SET status = EXCLUDED.status`,
		tenant, record.GetSellerId(), record.GetStatus(), record.GetAudit().GetEtag())
	return err
}

// SaveSellerListing writes a seller listing, replacing the stored one.
func (st *Store) SaveSellerListing(ctx context.Context, tenant string, record *sellersv1.SellerListing) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO seller_listings (tenant_id, seller_listing_id, code, name, status, capacity, etag) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (tenant_id, seller_listing_id) DO UPDATE SET code = EXCLUDED.code, name = EXCLUDED.name, status = EXCLUDED.status, capacity = EXCLUDED.capacity`,
		tenant, record.GetSellerListingId(), record.GetCode(), record.GetName(), record.GetStatus(), record.GetCapacity(), record.GetAudit().GetEtag())
	return err
}

// SaveSellerPayoutSchedule writes a seller payout schedule, replacing the stored one.
func (st *Store) SaveSellerPayoutSchedule(ctx context.Context, tenant string, record *sellersv1.SellerPayoutSchedule) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO seller_payout_schedules (tenant_id, seller_payout_schedule_id, title, status, etag) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (tenant_id, seller_payout_schedule_id) DO UPDATE SET title = EXCLUDED.title, status = EXCLUDED.status`,
		tenant, record.GetSellerPayoutScheduleId(), record.GetTitle(), record.GetStatus(), record.GetAudit().GetEtag())
	return err
}

// SaveSellerScorecard writes a seller scorecard, replacing the stored one.
func (st *Store) SaveSellerScorecard(ctx context.Context, tenant string, record *sellersv1.SellerScorecard) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO seller_scorecards (tenant_id, seller_scorecard_id, number, status, notes, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, seller_scorecard_id) DO UPDATE SET number = EXCLUDED.number, status = EXCLUDED.status, notes = EXCLUDED.notes`,
		tenant, record.GetSellerScorecardId(), record.GetNumber(), record.GetStatus(), record.GetNotes(), record.GetAudit().GetEtag())
	return err
}
