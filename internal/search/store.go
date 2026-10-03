package search

import (
	"context"
	"database/sql"

	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
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

// SaveSearchIndex writes a search index, replacing the stored one.
func (st *Store) SaveSearchIndex(ctx context.Context, tenant string, record *searchv1.SearchIndex) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO search_indexes (tenant_id, search_index_id, code, name, status, capacity, etag) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (tenant_id, search_index_id) DO UPDATE SET code = EXCLUDED.code, name = EXCLUDED.name, status = EXCLUDED.status, capacity = EXCLUDED.capacity`,
		tenant, record.GetSearchIndexId(), record.GetCode(), record.GetName(), record.GetStatus(), record.GetCapacity(), record.GetAudit().GetEtag())
	return err
}

// SaveSearchQuery writes a search query, replacing the stored one.
func (st *Store) SaveSearchQuery(ctx context.Context, tenant string, record *searchv1.SearchQuery) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO search_queries (tenant_id, search_query_id, kind, source, correlation_id, sequence) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, search_query_id) DO UPDATE SET kind = EXCLUDED.kind, source = EXCLUDED.source, correlation_id = EXCLUDED.correlation_id, sequence = EXCLUDED.sequence`,
		tenant, record.GetSearchQueryId(), record.GetKind(), record.GetSource(), record.GetCorrelationId(), record.GetSequence())
	return err
}

// SaveSynonym writes a synonym, replacing the stored one.
func (st *Store) SaveSynonym(ctx context.Context, tenant string, record *searchv1.Synonym) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO synonyms (tenant_id, synonym_id, name, description, status, priority, version, etag) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (tenant_id, synonym_id) DO UPDATE SET name = EXCLUDED.name, description = EXCLUDED.description, status = EXCLUDED.status, priority = EXCLUDED.priority, version = EXCLUDED.version`,
		tenant, record.GetSynonymId(), record.GetName(), record.GetDescription(), record.GetStatus(), record.GetPriority(), record.GetVersion(), record.GetAudit().GetEtag())
	return err
}
