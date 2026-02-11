package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool" //
)

func main() {
	// 1. Grab the custom variable we set in CMD
	connStr := os.Getenv("DATABASE_URL_DELTASYNC")
	if connStr == "" {
		log.Fatal("❌ Error: DATABASE_URL_DELTASYNC is empty. Did you run 'set' in this terminal?")
	}

	fmt.Println("Attempting to connect to Neon...")

	// 2. Create the pool
	pool, err := pgxpool.New(context.Background(), connStr)
	if err != nil {
		log.Fatalf("❌ Unable to create pool: %v\n", err)
	}
	defer pool.Close()

	// 3. Perform a physical Ping
	err = pool.Ping(context.Background())
	if err != nil {
		log.Fatalf("❌ Ping failed! Check your password or SSL mode: %v\n", err)
	}

	// 4. Run a simple query to verify table access
	var tableName string
	err = pool.QueryRow(context.Background(), 
		"SELECT table_name FROM information_schema.tables WHERE table_name = 'chunks'").Scan(&tableName)
	
	if err != nil {
		fmt.Println("⚠️ Connected, but couldn't find the 'chunks' table. Did you run the SQL in Neon console?")
	} else {
		fmt.Printf("🚀 Success! Connected to Neon and found table: %s\n", tableName)
	}
}