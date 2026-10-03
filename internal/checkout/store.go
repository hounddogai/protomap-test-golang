package checkout

import (
	"context"
	"database/sql"

	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
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

// SaveCheckoutSession writes a checkout session, replacing the stored one.
func (st *Store) SaveCheckoutSession(ctx context.Context, tenant string, record *checkoutv1.CheckoutSession) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO checkout_sessions (tenant_id, checkout_session_id, number, status, notes, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, checkout_session_id) DO UPDATE SET number = EXCLUDED.number, status = EXCLUDED.status, notes = EXCLUDED.notes`,
		tenant, record.GetCheckoutSessionId(), record.GetNumber(), record.GetStatus(), record.GetNotes(), record.GetAudit().GetEtag())
	return err
}

// SaveShippingQuote writes a shipping quote, replacing the stored one.
func (st *Store) SaveShippingQuote(ctx context.Context, tenant string, record *checkoutv1.ShippingQuote) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO shipping_quotes (tenant_id, shipping_quote_id, number, status, notes, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, shipping_quote_id) DO UPDATE SET number = EXCLUDED.number, status = EXCLUDED.status, notes = EXCLUDED.notes`,
		tenant, record.GetShippingQuoteId(), record.GetNumber(), record.GetStatus(), record.GetNotes(), record.GetAudit().GetEtag())
	return err
}

// SavePaymentIntent writes a payment intent, replacing the stored one.
func (st *Store) SavePaymentIntent(ctx context.Context, tenant string, record *checkoutv1.PaymentIntent) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO payment_intents (tenant_id, payment_intent_id, subject, status, priority, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, payment_intent_id) DO UPDATE SET subject = EXCLUDED.subject, status = EXCLUDED.status, priority = EXCLUDED.priority`,
		tenant, record.GetPaymentIntentId(), record.GetSubject(), record.GetStatus(), record.GetPriority(), record.GetAudit().GetEtag())
	return err
}
