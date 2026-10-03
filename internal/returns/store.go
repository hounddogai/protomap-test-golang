package returns

import (
	"context"
	"database/sql"

	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
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

// SaveReturnRequest writes a return request, replacing the stored one.
func (st *Store) SaveReturnRequest(ctx context.Context, tenant string, record *returnsv1.ReturnRequest) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO return_requests (tenant_id, return_request_id, number, status, notes, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, return_request_id) DO UPDATE SET number = EXCLUDED.number, status = EXCLUDED.status, notes = EXCLUDED.notes`,
		tenant, record.GetReturnRequestId(), record.GetNumber(), record.GetStatus(), record.GetNotes(), record.GetAudit().GetEtag())
	return err
}

// SaveReturnLabel writes a return label, replacing the stored one.
func (st *Store) SaveReturnLabel(ctx context.Context, tenant string, record *returnsv1.ReturnLabel) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO return_labels (tenant_id, return_label_id, number, status, notes, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, return_label_id) DO UPDATE SET number = EXCLUDED.number, status = EXCLUDED.status, notes = EXCLUDED.notes`,
		tenant, record.GetReturnLabelId(), record.GetNumber(), record.GetStatus(), record.GetNotes(), record.GetAudit().GetEtag())
	return err
}

// SaveInspection writes a inspection, replacing the stored one.
func (st *Store) SaveInspection(ctx context.Context, tenant string, record *returnsv1.Inspection) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO inspections (tenant_id, inspection_id, subject, status, priority, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, inspection_id) DO UPDATE SET subject = EXCLUDED.subject, status = EXCLUDED.status, priority = EXCLUDED.priority`,
		tenant, record.GetInspectionId(), record.GetSubject(), record.GetStatus(), record.GetPriority(), record.GetAudit().GetEtag())
	return err
}

// SaveRestockDecision writes a restock decision, replacing the stored one.
func (st *Store) SaveRestockDecision(ctx context.Context, tenant string, record *returnsv1.RestockDecision) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO restock_decisions (tenant_id, restock_decision_id, name, description, status, priority, version, etag) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (tenant_id, restock_decision_id) DO UPDATE SET name = EXCLUDED.name, description = EXCLUDED.description, status = EXCLUDED.status, priority = EXCLUDED.priority, version = EXCLUDED.version`,
		tenant, record.GetRestockDecisionId(), record.GetName(), record.GetDescription(), record.GetStatus(), record.GetPriority(), record.GetVersion(), record.GetAudit().GetEtag())
	return err
}
