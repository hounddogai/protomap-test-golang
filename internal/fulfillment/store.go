package fulfillment

import (
	"context"
	"database/sql"

	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
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

// SaveFulfillment writes a fulfillment, replacing the stored one.
func (st *Store) SaveFulfillment(ctx context.Context, tenant string, record *fulfillmentv1.Fulfillment) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO fulfillments (tenant_id, fulfillment_id, number, status, notes, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, fulfillment_id) DO UPDATE SET number = EXCLUDED.number, status = EXCLUDED.status, notes = EXCLUDED.notes`,
		tenant, record.GetFulfillmentId(), record.GetNumber(), record.GetStatus(), record.GetNotes(), record.GetAudit().GetEtag())
	return err
}

// SavePackingSlip writes a packing slip, replacing the stored one.
func (st *Store) SavePackingSlip(ctx context.Context, tenant string, record *fulfillmentv1.PackingSlip) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO packing_slips (tenant_id, packing_slip_id, number, status, notes, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, packing_slip_id) DO UPDATE SET number = EXCLUDED.number, status = EXCLUDED.status, notes = EXCLUDED.notes`,
		tenant, record.GetPackingSlipId(), record.GetNumber(), record.GetStatus(), record.GetNotes(), record.GetAudit().GetEtag())
	return err
}

// SaveFulfillmentException writes a fulfillment exception, replacing the stored one.
func (st *Store) SaveFulfillmentException(ctx context.Context, tenant string, record *fulfillmentv1.FulfillmentException) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO fulfillment_exceptions (tenant_id, fulfillment_exception_id, subject, status, priority, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, fulfillment_exception_id) DO UPDATE SET subject = EXCLUDED.subject, status = EXCLUDED.status, priority = EXCLUDED.priority`,
		tenant, record.GetFulfillmentExceptionId(), record.GetSubject(), record.GetStatus(), record.GetPriority(), record.GetAudit().GetEtag())
	return err
}
