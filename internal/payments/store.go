package payments

import (
	"context"
	"database/sql"

	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
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

// SavePayment writes a payment, replacing the stored one.
func (st *Store) SavePayment(ctx context.Context, tenant string, record *paymentsv1.Payment) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO payments (tenant_id, payment_id, number, status, notes, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, payment_id) DO UPDATE SET number = EXCLUDED.number, status = EXCLUDED.status, notes = EXCLUDED.notes`,
		tenant, record.GetPaymentId(), record.GetNumber(), record.GetStatus(), record.GetNotes(), record.GetAudit().GetEtag())
	return err
}

// SaveRefund writes a refund, replacing the stored one.
func (st *Store) SaveRefund(ctx context.Context, tenant string, record *paymentsv1.Refund) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO refunds (tenant_id, refund_id, number, status, notes, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, refund_id) DO UPDATE SET number = EXCLUDED.number, status = EXCLUDED.status, notes = EXCLUDED.notes`,
		tenant, record.GetRefundId(), record.GetNumber(), record.GetStatus(), record.GetNotes(), record.GetAudit().GetEtag())
	return err
}

// SavePaymentInstrument writes a payment instrument, replacing the stored one.
func (st *Store) SavePaymentInstrument(ctx context.Context, tenant string, record *paymentsv1.PaymentInstrument) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO payment_instruments (tenant_id, payment_instrument_id, status, currency_code, etag) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (tenant_id, payment_instrument_id) DO UPDATE SET status = EXCLUDED.status, currency_code = EXCLUDED.currency_code`,
		tenant, record.GetPaymentInstrumentId(), record.GetStatus(), record.GetCurrencyCode(), record.GetAudit().GetEtag())
	return err
}

// SavePayout writes a payout, replacing the stored one.
func (st *Store) SavePayout(ctx context.Context, tenant string, record *paymentsv1.Payout) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO payouts (tenant_id, payout_id, number, status, notes, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, payout_id) DO UPDATE SET number = EXCLUDED.number, status = EXCLUDED.status, notes = EXCLUDED.notes`,
		tenant, record.GetPayoutId(), record.GetNumber(), record.GetStatus(), record.GetNotes(), record.GetAudit().GetEtag())
	return err
}

// SaveDispute writes a dispute, replacing the stored one.
func (st *Store) SaveDispute(ctx context.Context, tenant string, record *paymentsv1.Dispute) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO disputes (tenant_id, dispute_id, subject, status, priority, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, dispute_id) DO UPDATE SET subject = EXCLUDED.subject, status = EXCLUDED.status, priority = EXCLUDED.priority`,
		tenant, record.GetDisputeId(), record.GetSubject(), record.GetStatus(), record.GetPriority(), record.GetAudit().GetEtag())
	return err
}
