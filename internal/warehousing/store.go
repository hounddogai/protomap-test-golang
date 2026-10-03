package warehousing

import (
	"context"
	"database/sql"

	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
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

// SaveWarehouse writes a warehouse, replacing the stored one.
func (st *Store) SaveWarehouse(ctx context.Context, tenant string, record *warehousingv1.Warehouse) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO warehouses (tenant_id, warehouse_id, code, name, status, capacity, etag) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (tenant_id, warehouse_id) DO UPDATE SET code = EXCLUDED.code, name = EXCLUDED.name, status = EXCLUDED.status, capacity = EXCLUDED.capacity`,
		tenant, record.GetWarehouseId(), record.GetCode(), record.GetName(), record.GetStatus(), record.GetCapacity(), record.GetAudit().GetEtag())
	return err
}

// SaveStorageBin writes a storage bin, replacing the stored one.
func (st *Store) SaveStorageBin(ctx context.Context, tenant string, record *warehousingv1.StorageBin) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO storage_bins (tenant_id, storage_bin_id, name, parent_id, depth, status, etag) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (tenant_id, storage_bin_id) DO UPDATE SET name = EXCLUDED.name, parent_id = EXCLUDED.parent_id, depth = EXCLUDED.depth, status = EXCLUDED.status`,
		tenant, record.GetStorageBinId(), record.GetName(), record.GetParentId(), record.GetDepth(), record.GetStatus(), record.GetAudit().GetEtag())
	return err
}

// SavePickList writes a pick list, replacing the stored one.
func (st *Store) SavePickList(ctx context.Context, tenant string, record *warehousingv1.PickList) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO pick_lists (tenant_id, pick_list_id, number, status, notes, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, pick_list_id) DO UPDATE SET number = EXCLUDED.number, status = EXCLUDED.status, notes = EXCLUDED.notes`,
		tenant, record.GetPickListId(), record.GetNumber(), record.GetStatus(), record.GetNotes(), record.GetAudit().GetEtag())
	return err
}

// SavePutawayTask writes a putaway task, replacing the stored one.
func (st *Store) SavePutawayTask(ctx context.Context, tenant string, record *warehousingv1.PutawayTask) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO putaway_tasks (tenant_id, putaway_task_id, subject, status, priority, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, putaway_task_id) DO UPDATE SET subject = EXCLUDED.subject, status = EXCLUDED.status, priority = EXCLUDED.priority`,
		tenant, record.GetPutawayTaskId(), record.GetSubject(), record.GetStatus(), record.GetPriority(), record.GetAudit().GetEtag())
	return err
}
