package giftcards

import (
	"context"
	"database/sql"

	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
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

// SaveGiftCard writes a gift card, replacing the stored one.
func (st *Store) SaveGiftCard(ctx context.Context, tenant string, record *giftcardsv1.GiftCard) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO gift_cards (tenant_id, gift_card_id, status, currency_code, etag) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (tenant_id, gift_card_id) DO UPDATE SET status = EXCLUDED.status, currency_code = EXCLUDED.currency_code`,
		tenant, record.GetGiftCardId(), record.GetStatus(), record.GetCurrencyCode(), record.GetAudit().GetEtag())
	return err
}

// SaveGiftCardBatch writes a gift card batch, replacing the stored one.
func (st *Store) SaveGiftCardBatch(ctx context.Context, tenant string, record *giftcardsv1.GiftCardBatch) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO gift_card_batches (tenant_id, gift_card_batch_id, number, status, notes, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, gift_card_batch_id) DO UPDATE SET number = EXCLUDED.number, status = EXCLUDED.status, notes = EXCLUDED.notes`,
		tenant, record.GetGiftCardBatchId(), record.GetNumber(), record.GetStatus(), record.GetNotes(), record.GetAudit().GetEtag())
	return err
}

// SaveGiftCardTransaction writes a gift card transaction, replacing the stored one.
func (st *Store) SaveGiftCardTransaction(ctx context.Context, tenant string, record *giftcardsv1.GiftCardTransaction) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO gift_card_transactions (tenant_id, gift_card_transaction_id, kind, source, correlation_id, sequence) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, gift_card_transaction_id) DO UPDATE SET kind = EXCLUDED.kind, source = EXCLUDED.source, correlation_id = EXCLUDED.correlation_id, sequence = EXCLUDED.sequence`,
		tenant, record.GetGiftCardTransactionId(), record.GetKind(), record.GetSource(), record.GetCorrelationId(), record.GetSequence())
	return err
}
