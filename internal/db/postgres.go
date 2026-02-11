package db

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool" // Ensure you ran 'go get github.com/jackc/pgx/v5'
)

type RemoteDB struct {
	Pool *pgxpool.Pool
}

// InitPostgres connects to Neon and verifies the link with a Ping
func InitPostgres() *RemoteDB {
	connStr := os.Getenv("DATABASE_URL_DELTASYNC")
	if connStr == "" {
		log.Fatal("❌ Error: DATABASE_URL_DELTASYNC environment variable is not set")
	}

	// 1. Create the connection pool
	pool, err := pgxpool.New(context.Background(), connStr)
	if err != nil {
		log.Fatal("❌ Unable to create connection pool:", err)
	}

	// 2. The Ping Test: Confirms the server can actually "talk" to Neon
	err = pool.Ping(context.Background())
	if err != nil {
		log.Fatal("❌ Connection failed! Could not ping Neon database:", err)
	}

	fmt.Println("🚀 Success! Server is connected to Neon.")
	return &RemoteDB{Pool: pool}
}

// GetMissingChunks checks the 'chunks' table for existing fingerprints
func (db *RemoteDB) GetMissingChunks(hashes []string) ([]string, error) {
	var missing []string
	for _, hash := range hashes {
		var exists bool
		query := `SELECT EXISTS(SELECT 1 FROM chunks WHERE hash=$1);`
		err := db.Pool.QueryRow(context.Background(), query, hash).Scan(&exists)
		if err != nil {
			return nil, err
		}
		if !exists {
			missing = append(missing, hash)
		}
	}
	return missing, nil
}

// RegisterChunk saves the raw bytes directly to Neon
func (r *RemoteDB) RegisterChunk(hash string, data []byte, size int) error {
	// Added 'data' column to your previous logic to ensure the file is stored
	query := `INSERT INTO chunks (hash, data, size) 
			  VALUES ($1, $2, $3)
			  ON CONFLICT (hash) DO NOTHING`
	_, err := r.Pool.Exec(context.Background(), query, hash, data, size)
	return err
}

// UpdateFileRecipe uses Postgres native array support for the hash list
func (r *RemoteDB) UpdateFileRecipe(fileName string, hashes []string) error {
	query := `INSERT INTO file_recipes (file_name, chunk_hashes) 
			  VALUES ($1, $2) 
			  ON CONFLICT (file_name) 
			  DO UPDATE SET chunk_hashes = $2, updated_at = CURRENT_TIMESTAMP`
	
	// pgx handles []string -> TEXT[] automatically
	_, err := r.Pool.Exec(context.Background(), query, fileName, hashes)
	return err
}

// GetAllRecipes retrieves all files for the dashboard
func (r *RemoteDB) GetAllRecipes() ([]struct{Name string; UpdatedAt time.Time}, error) {
	rows, err := r.Pool.Query(context.Background(), "SELECT file_name, updated_at FROM file_recipes ORDER BY updated_at DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []struct{Name string; UpdatedAt time.Time}
	for rows.Next() {
		var item struct{Name string; UpdatedAt time.Time}
		if err := rows.Scan(&item.Name, &item.UpdatedAt); err != nil {
			return nil, err
		}
		results = append(results, item)
	}
	return results, nil
}