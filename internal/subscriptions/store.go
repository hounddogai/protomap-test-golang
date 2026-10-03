package subscriptions

import (
	"context"
	"database/sql"

	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
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

// SaveSubscription writes a subscription, replacing the stored one.
func (st *Store) SaveSubscription(ctx context.Context, tenant string, record *subscriptionsv1.Subscription) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO subscriptions (tenant_id, subscription_id, status, currency_code, etag) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (tenant_id, subscription_id) DO UPDATE SET status = EXCLUDED.status, currency_code = EXCLUDED.currency_code`,
		tenant, record.GetSubscriptionId(), record.GetStatus(), record.GetCurrencyCode(), record.GetAudit().GetEtag())
	return err
}

// SaveSubscriptionPlan writes a subscription plan, replacing the stored one.
func (st *Store) SaveSubscriptionPlan(ctx context.Context, tenant string, record *subscriptionsv1.SubscriptionPlan) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO subscription_plans (tenant_id, subscription_plan_id, name, description, status, priority, version, etag) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (tenant_id, subscription_plan_id) DO UPDATE SET name = EXCLUDED.name, description = EXCLUDED.description, status = EXCLUDED.status, priority = EXCLUDED.priority, version = EXCLUDED.version`,
		tenant, record.GetSubscriptionPlanId(), record.GetName(), record.GetDescription(), record.GetStatus(), record.GetPriority(), record.GetVersion(), record.GetAudit().GetEtag())
	return err
}

// SaveRenewalSchedule writes a renewal schedule, replacing the stored one.
func (st *Store) SaveRenewalSchedule(ctx context.Context, tenant string, record *subscriptionsv1.RenewalSchedule) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO renewal_schedules (tenant_id, renewal_schedule_id, title, status, etag) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (tenant_id, renewal_schedule_id) DO UPDATE SET title = EXCLUDED.title, status = EXCLUDED.status`,
		tenant, record.GetRenewalScheduleId(), record.GetTitle(), record.GetStatus(), record.GetAudit().GetEtag())
	return err
}

// SaveUsageRecord writes a usage record, replacing the stored one.
func (st *Store) SaveUsageRecord(ctx context.Context, tenant string, record *subscriptionsv1.UsageRecord) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO usage_records (tenant_id, usage_record_id, kind, source, correlation_id, sequence) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, usage_record_id) DO UPDATE SET kind = EXCLUDED.kind, source = EXCLUDED.source, correlation_id = EXCLUDED.correlation_id, sequence = EXCLUDED.sequence`,
		tenant, record.GetUsageRecordId(), record.GetKind(), record.GetSource(), record.GetCorrelationId(), record.GetSequence())
	return err
}
