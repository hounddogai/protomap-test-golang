package pricing

import (
	"context"
	"database/sql"

	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
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

// SavePriceList writes a price list, replacing the stored one.
func (st *Store) SavePriceList(ctx context.Context, tenant string, record *pricingv1.PriceList) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO price_lists (tenant_id, price_list_id, name, description, status, priority, version, etag) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (tenant_id, price_list_id) DO UPDATE SET name = EXCLUDED.name, description = EXCLUDED.description, status = EXCLUDED.status, priority = EXCLUDED.priority, version = EXCLUDED.version`,
		tenant, record.GetPriceListId(), record.GetName(), record.GetDescription(), record.GetStatus(), record.GetPriority(), record.GetVersion(), record.GetAudit().GetEtag())
	return err
}

// SavePriceQuote writes a price quote, replacing the stored one.
func (st *Store) SavePriceQuote(ctx context.Context, tenant string, record *pricingv1.PriceQuote) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO price_quotes (tenant_id, price_quote_id, number, status, notes, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, price_quote_id) DO UPDATE SET number = EXCLUDED.number, status = EXCLUDED.status, notes = EXCLUDED.notes`,
		tenant, record.GetPriceQuoteId(), record.GetNumber(), record.GetStatus(), record.GetNotes(), record.GetAudit().GetEtag())
	return err
}

// SavePriceOverride writes a price override, replacing the stored one.
func (st *Store) SavePriceOverride(ctx context.Context, tenant string, record *pricingv1.PriceOverride) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO price_overrides (tenant_id, price_override_id, name, description, status, priority, version, etag) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (tenant_id, price_override_id) DO UPDATE SET name = EXCLUDED.name, description = EXCLUDED.description, status = EXCLUDED.status, priority = EXCLUDED.priority, version = EXCLUDED.version`,
		tenant, record.GetPriceOverrideId(), record.GetName(), record.GetDescription(), record.GetStatus(), record.GetPriority(), record.GetVersion(), record.GetAudit().GetEtag())
	return err
}

// SaveCurrencyRate writes a currency rate, replacing the stored one.
func (st *Store) SaveCurrencyRate(ctx context.Context, tenant string, record *pricingv1.CurrencyRate) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO currency_rates (tenant_id, currency_rate_id, kind, source, correlation_id, sequence) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, currency_rate_id) DO UPDATE SET kind = EXCLUDED.kind, source = EXCLUDED.source, correlation_id = EXCLUDED.correlation_id, sequence = EXCLUDED.sequence`,
		tenant, record.GetCurrencyRateId(), record.GetKind(), record.GetSource(), record.GetCorrelationId(), record.GetSequence())
	return err
}
