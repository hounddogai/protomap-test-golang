package catalog

import (
	"context"
	"database/sql"

	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
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

// SaveProduct writes a product, replacing the stored one.
func (st *Store) SaveProduct(ctx context.Context, tenant string, record *catalogv1.Product) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO products (tenant_id, product_id, code, name, status, capacity, etag) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (tenant_id, product_id) DO UPDATE SET code = EXCLUDED.code, name = EXCLUDED.name, status = EXCLUDED.status, capacity = EXCLUDED.capacity`,
		tenant, record.GetProductId(), record.GetCode(), record.GetName(), record.GetStatus(), record.GetCapacity(), record.GetAudit().GetEtag())
	return err
}

// SaveCategory writes a category, replacing the stored one.
func (st *Store) SaveCategory(ctx context.Context, tenant string, record *catalogv1.Category) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO categories (tenant_id, category_id, name, parent_id, depth, status, etag) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (tenant_id, category_id) DO UPDATE SET name = EXCLUDED.name, parent_id = EXCLUDED.parent_id, depth = EXCLUDED.depth, status = EXCLUDED.status`,
		tenant, record.GetCategoryId(), record.GetName(), record.GetParentId(), record.GetDepth(), record.GetStatus(), record.GetAudit().GetEtag())
	return err
}

// SaveProductVariant writes a product variant, replacing the stored one.
func (st *Store) SaveProductVariant(ctx context.Context, tenant string, record *catalogv1.ProductVariant) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO product_variants (tenant_id, product_variant_id, code, name, status, capacity, etag) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (tenant_id, product_variant_id) DO UPDATE SET code = EXCLUDED.code, name = EXCLUDED.name, status = EXCLUDED.status, capacity = EXCLUDED.capacity`,
		tenant, record.GetProductVariantId(), record.GetCode(), record.GetName(), record.GetStatus(), record.GetCapacity(), record.GetAudit().GetEtag())
	return err
}

// SaveBrand writes a brand, replacing the stored one.
func (st *Store) SaveBrand(ctx context.Context, tenant string, record *catalogv1.Brand) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO brands (tenant_id, brand_id, status, etag) VALUES ($1, $2, $3, $4)
ON CONFLICT (tenant_id, brand_id) DO UPDATE SET status = EXCLUDED.status`,
		tenant, record.GetBrandId(), record.GetStatus(), record.GetAudit().GetEtag())
	return err
}

// SaveAttribute writes a attribute, replacing the stored one.
func (st *Store) SaveAttribute(ctx context.Context, tenant string, record *catalogv1.Attribute) error {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO attributes (tenant_id, attribute_id, name, description, status, priority, version, etag) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (tenant_id, attribute_id) DO UPDATE SET name = EXCLUDED.name, description = EXCLUDED.description, status = EXCLUDED.status, priority = EXCLUDED.priority, version = EXCLUDED.version`,
		tenant, record.GetAttributeId(), record.GetName(), record.GetDescription(), record.GetStatus(), record.GetPriority(), record.GetVersion(), record.GetAudit().GetEtag())
	return err
}
