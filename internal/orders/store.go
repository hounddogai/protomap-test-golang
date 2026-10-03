package orders

import (
	"context"
	"database/sql"

	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
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

// SaveOrder writes a order, replacing the stored one.
func (st *Store) SaveOrder(ctx context.Context, tenant string, record *ordersv1.Order) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO orders (tenant_id, order_id, number, status, notes, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, order_id) DO UPDATE SET number = EXCLUDED.number, status = EXCLUDED.status, notes = EXCLUDED.notes`,
		tenant, record.GetOrderId(), record.GetNumber(), record.GetStatus(), record.GetNotes(), record.GetAudit().GetEtag())
	return err
}

// SaveOrderAmendment writes a order amendment, replacing the stored one.
func (st *Store) SaveOrderAmendment(ctx context.Context, tenant string, record *ordersv1.OrderAmendment) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO order_amendments (tenant_id, order_amendment_id, number, status, notes, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, order_amendment_id) DO UPDATE SET number = EXCLUDED.number, status = EXCLUDED.status, notes = EXCLUDED.notes`,
		tenant, record.GetOrderAmendmentId(), record.GetNumber(), record.GetStatus(), record.GetNotes(), record.GetAudit().GetEtag())
	return err
}

// SaveBackorder writes a backorder, replacing the stored one.
func (st *Store) SaveBackorder(ctx context.Context, tenant string, record *ordersv1.Backorder) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO backorders (tenant_id, backorder_id, subject, status, priority, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, backorder_id) DO UPDATE SET subject = EXCLUDED.subject, status = EXCLUDED.status, priority = EXCLUDED.priority`,
		tenant, record.GetBackorderId(), record.GetSubject(), record.GetStatus(), record.GetPriority(), record.GetAudit().GetEtag())
	return err
}

// SaveOrderNote writes a order note, replacing the stored one.
func (st *Store) SaveOrderNote(ctx context.Context, tenant string, record *ordersv1.OrderNote) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO order_notes (tenant_id, order_note_id, kind, source, correlation_id, sequence) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, order_note_id) DO UPDATE SET kind = EXCLUDED.kind, source = EXCLUDED.source, correlation_id = EXCLUDED.correlation_id, sequence = EXCLUDED.sequence`,
		tenant, record.GetOrderNoteId(), record.GetKind(), record.GetSource(), record.GetCorrelationId(), record.GetSequence())
	return err
}
