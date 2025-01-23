package collector

import (
	"fmt"

	"github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surreallogs/internal/config"
)

func connectSurrealDB(cfg *config.SurrealDB) (*surrealdb.DB, error) {
	db, err := surrealdb.New(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to create SurrealDB client: %w", err)
	}

	token, err := db.SignIn(&surrealdb.Auth{
		Username: cfg.User,
		Password: cfg.Pass,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to sign in to SurrealDB: %w", err)
	}

	if err := db.Authenticate(token); err != nil {
		return nil, fmt.Errorf("failed to authenticate with SurrealDB: %w", err)
	}

	if err := db.Use(cfg.Namespace, cfg.Database); err != nil {
		return nil, fmt.Errorf("failed to use namespace/database: %w", err)
	}

	return db, nil
}
