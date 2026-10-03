package reviews

import (
	"context"
	"database/sql"

	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
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

// SaveProductReview writes a product review, replacing the stored one.
func (st *Store) SaveProductReview(ctx context.Context, tenant string, record *reviewsv1.ProductReview) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO product_reviews (tenant_id, product_review_id, number, status, notes, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, product_review_id) DO UPDATE SET number = EXCLUDED.number, status = EXCLUDED.status, notes = EXCLUDED.notes`,
		tenant, record.GetProductReviewId(), record.GetNumber(), record.GetStatus(), record.GetNotes(), record.GetAudit().GetEtag())
	return err
}

// SaveReviewModeration writes a review moderation, replacing the stored one.
func (st *Store) SaveReviewModeration(ctx context.Context, tenant string, record *reviewsv1.ReviewModeration) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO review_moderations (tenant_id, review_moderation_id, subject, status, priority, etag) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, review_moderation_id) DO UPDATE SET subject = EXCLUDED.subject, status = EXCLUDED.status, priority = EXCLUDED.priority`,
		tenant, record.GetReviewModerationId(), record.GetSubject(), record.GetStatus(), record.GetPriority(), record.GetAudit().GetEtag())
	return err
}

// SaveReviewVote writes a review vote, replacing the stored one.
func (st *Store) SaveReviewVote(ctx context.Context, tenant string, record *reviewsv1.ReviewVote) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO review_votes (tenant_id, review_vote_id, kind, source, correlation_id, sequence) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, review_vote_id) DO UPDATE SET kind = EXCLUDED.kind, source = EXCLUDED.source, correlation_id = EXCLUDED.correlation_id, sequence = EXCLUDED.sequence`,
		tenant, record.GetReviewVoteId(), record.GetKind(), record.GetSource(), record.GetCorrelationId(), record.GetSequence())
	return err
}
