package db

import(
	"database/sql"
	"log"

	_ "github.com/glebarez/go-sqlite"  //CGO-free driver
)

type LocalDB struct{
	Conn *sql.DB
}

// InitSQLite sets up the local database file
func InitSQLite(dbPath string) *LocalDB{
	// open or create the sqlite file
	db, err := sql.Open("sqlite", dbPath)
	if err != nil{
		log.Fatal(err)
	}

	// create a table to store file metadata and their chunk recipes
	// we store the hashes as a comma-separated string for simplicity\
	query := `
	CREATE TABLE IF NOT EXISTS file_index (
			path TEXT PRIMARY KEY,
			last_modified DATETIME,
			chunk_hashes TEXT
	);`

	_, err = db.Exec(query)
	if err != nil {
		log.Fatal(err)
	}

	return &LocalDB{Conn: db}
}

// SaveFileIndex stores the file's current state
func (db *LocalDB) SaveFileIndex(path string, hashes string) error {
	query := `INSERT OR REPLACE INTO file_index (path, last_modified, chunk_hashes) VALUES (?, CURRENT_TIMESTAMP, ?);`
	_, err := db.Conn.Exec(query, path, hashes)
	return err
}