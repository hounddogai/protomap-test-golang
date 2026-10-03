package customers

import (
	"context"
	"database/sql"

	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
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

// SaveCustomer writes a customer, replacing the stored one.
func (st *Store) SaveCustomer(ctx context.Context, tenant string, record *customersv1.Customer) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO customers (tenant_id, customer_id, status, etag) VALUES ($1, $2, $3, $4)
ON CONFLICT (tenant_id, customer_id) DO UPDATE SET status = EXCLUDED.status`,
		tenant, record.GetCustomerId(), record.GetStatus(), record.GetAudit().GetEtag())
	return err
}

// SaveHousehold writes a household, replacing the stored one.
func (st *Store) SaveHousehold(ctx context.Context, tenant string, record *customersv1.Household) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO households (tenant_id, household_id, name, parent_id, depth, status, etag) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (tenant_id, household_id) DO UPDATE SET name = EXCLUDED.name, parent_id = EXCLUDED.parent_id, depth = EXCLUDED.depth, status = EXCLUDED.status`,
		tenant, record.GetHouseholdId(), record.GetName(), record.GetParentId(), record.GetDepth(), record.GetStatus(), record.GetAudit().GetEtag())
	return err
}

// SaveConsentRecord writes a consent record, replacing the stored one.
func (st *Store) SaveConsentRecord(ctx context.Context, tenant string, record *customersv1.ConsentRecord) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO consent_records (tenant_id, consent_record_id, kind, source, correlation_id, sequence) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, consent_record_id) DO UPDATE SET kind = EXCLUDED.kind, source = EXCLUDED.source, correlation_id = EXCLUDED.correlation_id, sequence = EXCLUDED.sequence`,
		tenant, record.GetConsentRecordId(), record.GetKind(), record.GetSource(), record.GetCorrelationId(), record.GetSequence())
	return err
}

// SaveCustomerSegment writes a customer segment, replacing the stored one.
func (st *Store) SaveCustomerSegment(ctx context.Context, tenant string, record *customersv1.CustomerSegment) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO customer_segments (tenant_id, customer_segment_id, name, description, status, priority, version, etag) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (tenant_id, customer_segment_id) DO UPDATE SET name = EXCLUDED.name, description = EXCLUDED.description, status = EXCLUDED.status, priority = EXCLUDED.priority, version = EXCLUDED.version`,
		tenant, record.GetCustomerSegmentId(), record.GetName(), record.GetDescription(), record.GetStatus(), record.GetPriority(), record.GetVersion(), record.GetAudit().GetEtag())
	return err
}

// SavePreference writes a preference, replacing the stored one.
func (st *Store) SavePreference(ctx context.Context, tenant string, record *customersv1.Preference) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO preferences (tenant_id, preference_id, name, description, status, priority, version, etag) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (tenant_id, preference_id) DO UPDATE SET name = EXCLUDED.name, description = EXCLUDED.description, status = EXCLUDED.status, priority = EXCLUDED.priority, version = EXCLUDED.version`,
		tenant, record.GetPreferenceId(), record.GetName(), record.GetDescription(), record.GetStatus(), record.GetPriority(), record.GetVersion(), record.GetAudit().GetEtag())
	return err
}
