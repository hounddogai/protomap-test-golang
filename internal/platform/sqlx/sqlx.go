// Package sqlx opens the monolith's database.
package sqlx

import (
	"context"
	"database/sql"
	"time"
)

// Open opens the database pool. The driver registers itself in the binary's main package.
func Open(ctx context.Context, driver, dsn string) (*sql.DB, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(64)
	db.SetConnMaxIdleTime(5 * time.Minute)
	return db, nil
}
