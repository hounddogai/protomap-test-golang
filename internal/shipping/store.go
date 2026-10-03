package shipping

import (
	"context"
	"database/sql"

	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
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

// SaveShipment writes a shipment, replacing the stored one.
func (st *Store) SaveShipment(ctx context.Context, tenant string, record *shippingv1.Shipment) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO shipments (tenant_id, shipment_id, number, status, notes, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, shipment_id) DO UPDATE SET number = EXCLUDED.number, status = EXCLUDED.status, notes = EXCLUDED.notes`,
		tenant, record.GetShipmentId(), record.GetNumber(), record.GetStatus(), record.GetNotes(), record.GetAudit().GetEtag())
	return err
}

// SaveParcel writes a parcel, replacing the stored one.
func (st *Store) SaveParcel(ctx context.Context, tenant string, record *shippingv1.Parcel) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO parcels (tenant_id, parcel_id, code, name, status, capacity, etag) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (tenant_id, parcel_id) DO UPDATE SET code = EXCLUDED.code, name = EXCLUDED.name, status = EXCLUDED.status, capacity = EXCLUDED.capacity`,
		tenant, record.GetParcelId(), record.GetCode(), record.GetName(), record.GetStatus(), record.GetCapacity(), record.GetAudit().GetEtag())
	return err
}

// SaveCarrierRate writes a carrier rate, replacing the stored one.
func (st *Store) SaveCarrierRate(ctx context.Context, tenant string, record *shippingv1.CarrierRate) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO carrier_rates (tenant_id, carrier_rate_id, name, description, status, priority, version, etag) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (tenant_id, carrier_rate_id) DO UPDATE SET name = EXCLUDED.name, description = EXCLUDED.description, status = EXCLUDED.status, priority = EXCLUDED.priority, version = EXCLUDED.version`,
		tenant, record.GetCarrierRateId(), record.GetName(), record.GetDescription(), record.GetStatus(), record.GetPriority(), record.GetVersion(), record.GetAudit().GetEtag())
	return err
}

// SaveTrackingEvent writes a tracking event, replacing the stored one.
func (st *Store) SaveTrackingEvent(ctx context.Context, tenant string, record *shippingv1.TrackingEvent) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO tracking_events (tenant_id, tracking_event_id, kind, source, correlation_id, sequence) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, tracking_event_id) DO UPDATE SET kind = EXCLUDED.kind, source = EXCLUDED.source, correlation_id = EXCLUDED.correlation_id, sequence = EXCLUDED.sequence`,
		tenant, record.GetTrackingEventId(), record.GetKind(), record.GetSource(), record.GetCorrelationId(), record.GetSequence())
	return err
}

// SaveManifest writes a manifest, replacing the stored one.
func (st *Store) SaveManifest(ctx context.Context, tenant string, record *shippingv1.Manifest) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO manifests (tenant_id, manifest_id, number, status, notes, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, manifest_id) DO UPDATE SET number = EXCLUDED.number, status = EXCLUDED.status, notes = EXCLUDED.notes`,
		tenant, record.GetManifestId(), record.GetNumber(), record.GetStatus(), record.GetNotes(), record.GetAudit().GetEtag())
	return err
}
