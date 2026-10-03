package fraud

import (
	"context"
	"database/sql"

	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
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

// SaveRiskAssessment writes a risk assessment, replacing the stored one.
func (st *Store) SaveRiskAssessment(ctx context.Context, tenant string, record *fraudv1.RiskAssessment) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO risk_assessments (tenant_id, risk_assessment_id, subject, status, priority, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, risk_assessment_id) DO UPDATE SET subject = EXCLUDED.subject, status = EXCLUDED.status, priority = EXCLUDED.priority`,
		tenant, record.GetRiskAssessmentId(), record.GetSubject(), record.GetStatus(), record.GetPriority(), record.GetAudit().GetEtag())
	return err
}

// SaveFraudSignal writes a fraud signal, replacing the stored one.
func (st *Store) SaveFraudSignal(ctx context.Context, tenant string, record *fraudv1.FraudSignal) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO fraud_signals (tenant_id, fraud_signal_id, kind, source, correlation_id, sequence) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, fraud_signal_id) DO UPDATE SET kind = EXCLUDED.kind, source = EXCLUDED.source, correlation_id = EXCLUDED.correlation_id, sequence = EXCLUDED.sequence`,
		tenant, record.GetFraudSignalId(), record.GetKind(), record.GetSource(), record.GetCorrelationId(), record.GetSequence())
	return err
}

// SaveFraudRule writes a fraud rule, replacing the stored one.
func (st *Store) SaveFraudRule(ctx context.Context, tenant string, record *fraudv1.FraudRule) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO fraud_rules (tenant_id, fraud_rule_id, name, description, status, priority, version, etag) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (tenant_id, fraud_rule_id) DO UPDATE SET name = EXCLUDED.name, description = EXCLUDED.description, status = EXCLUDED.status, priority = EXCLUDED.priority, version = EXCLUDED.version`,
		tenant, record.GetFraudRuleId(), record.GetName(), record.GetDescription(), record.GetStatus(), record.GetPriority(), record.GetVersion(), record.GetAudit().GetEtag())
	return err
}

// SaveWatchlist writes a watchlist, replacing the stored one.
func (st *Store) SaveWatchlist(ctx context.Context, tenant string, record *fraudv1.Watchlist) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO watchlists (tenant_id, watchlist_id, name, parent_id, depth, status, etag) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (tenant_id, watchlist_id) DO UPDATE SET name = EXCLUDED.name, parent_id = EXCLUDED.parent_id, depth = EXCLUDED.depth, status = EXCLUDED.status`,
		tenant, record.GetWatchlistId(), record.GetName(), record.GetParentId(), record.GetDepth(), record.GetStatus(), record.GetAudit().GetEtag())
	return err
}
