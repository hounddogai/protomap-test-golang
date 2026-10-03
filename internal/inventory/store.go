package inventory

import (
	"context"
	"database/sql"

	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
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

// SaveStockItem writes a stock item, replacing the stored one.
func (st *Store) SaveStockItem(ctx context.Context, tenant string, record *inventoryv1.StockItem) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO stock_items (tenant_id, stock_item_id, code, name, status, capacity, etag) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (tenant_id, stock_item_id) DO UPDATE SET code = EXCLUDED.code, name = EXCLUDED.name, status = EXCLUDED.status, capacity = EXCLUDED.capacity`,
		tenant, record.GetStockItemId(), record.GetCode(), record.GetName(), record.GetStatus(), record.GetCapacity(), record.GetAudit().GetEtag())
	return err
}

// SaveStockReservation writes a stock reservation, replacing the stored one.
func (st *Store) SaveStockReservation(ctx context.Context, tenant string, record *inventoryv1.StockReservation) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO stock_reservations (tenant_id, stock_reservation_id, title, status, etag) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (tenant_id, stock_reservation_id) DO UPDATE SET title = EXCLUDED.title, status = EXCLUDED.status`,
		tenant, record.GetStockReservationId(), record.GetTitle(), record.GetStatus(), record.GetAudit().GetEtag())
	return err
}

// SaveStockMovement writes a stock movement, replacing the stored one.
func (st *Store) SaveStockMovement(ctx context.Context, tenant string, record *inventoryv1.StockMovement) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO stock_movements (tenant_id, stock_movement_id, kind, source, correlation_id, sequence) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, stock_movement_id) DO UPDATE SET kind = EXCLUDED.kind, source = EXCLUDED.source, correlation_id = EXCLUDED.correlation_id, sequence = EXCLUDED.sequence`,
		tenant, record.GetStockMovementId(), record.GetKind(), record.GetSource(), record.GetCorrelationId(), record.GetSequence())
	return err
}

// SaveReplenishmentPlan writes a replenishment plan, replacing the stored one.
func (st *Store) SaveReplenishmentPlan(ctx context.Context, tenant string, record *inventoryv1.ReplenishmentPlan) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO replenishment_plans (tenant_id, replenishment_plan_id, number, status, notes, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, replenishment_plan_id) DO UPDATE SET number = EXCLUDED.number, status = EXCLUDED.status, notes = EXCLUDED.notes`,
		tenant, record.GetReplenishmentPlanId(), record.GetNumber(), record.GetStatus(), record.GetNotes(), record.GetAudit().GetEtag())
	return err
}
