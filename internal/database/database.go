package database

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// Database represents a connection to the ExileDB SQLite database
type Database struct {
	db   *sql.DB
	path string
}

// DatabaseOptions configures database creation and connection behavior
type DatabaseOptions struct {
	// Path to the SQLite database file
	Path string

	// WALMode enables Write-Ahead Logging mode for better concurrency
	WALMode bool

	// ForeignKeys enables foreign key constraint checking
	ForeignKeys bool

	// BusyTimeout sets the timeout for locked database operations
	BusyTimeout time.Duration
}

// DefaultDatabaseOptions returns sensible default options for database connections
func DefaultDatabaseOptions(path string) *DatabaseOptions {
	return &DatabaseOptions{
		Path:        path,
		WALMode:     true,
		ForeignKeys: true,
		BusyTimeout: 30 * time.Second,
	}
}

// NewDatabase creates a new database connection with the given options
func NewDatabase(options *DatabaseOptions) (*Database, error) {
	if options == nil {
		return nil, fmt.Errorf("database options cannot be nil")
	}

	if options.Path == "" {
		return nil, fmt.Errorf("database path cannot be empty")
	}

	// Create the directory if it doesn't exist
	if err := ensureDirectory(options.Path); err != nil {
		return nil, fmt.Errorf("creating database directory: %w", err)
	}

	// Build connection string with pragmas
	connStr := buildConnectionString(options)

	// Open the database connection
	db, err := sql.Open("sqlite3", connStr)
	if err != nil {
		return nil, fmt.Errorf("opening database %s: %w", options.Path, err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("testing database connection: %w", err)
	}

	database := &Database{
		db:   db,
		path: options.Path,
	}

	return database, nil
}

// Close closes the database connection
func (d *Database) Close() error {
	if d.db == nil {
		return nil
	}

	err := d.db.Close()
	d.db = nil

	if err != nil {
		return fmt.Errorf("closing database connection: %w", err)
	}

	return nil
}

// BeginTx starts a new transaction with the given options
func (d *Database) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	if d.db == nil {
		return nil, fmt.Errorf("database connection is closed")
	}

	tx, err := d.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("starting transaction: %w", err)
	}

	return tx, nil
}

// Exec executes a SQL statement that doesn't return rows
func (d *Database) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if d.db == nil {
		return nil, fmt.Errorf("database connection is closed")
	}

	result, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("executing query: %w", err)
	}

	return result, nil
}

// Query executes a SQL query that returns rows
func (d *Database) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if d.db == nil {
		return nil, fmt.Errorf("database connection is closed")
	}

	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("executing query: %w", err)
	}

	return rows, nil
}

// QueryRow executes a SQL query that is expected to return at most one row
func (d *Database) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return d.db.QueryRowContext(ctx, query, args...)
}

// HasUserTables checks if the database contains any user tables (non-system and non-metadata tables)
func (d *Database) HasUserTables(ctx context.Context) (bool, error) {
	if d.db == nil {
		return false, fmt.Errorf("database connection is closed")
	}

	query := `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND substr(name, 1, 1) <> '_'`
	var count int
	row := d.QueryRow(ctx, query)
	if err := row.Scan(&count); err != nil {
		return false, fmt.Errorf("checking for user tables: %w", err)
	}

	return count > 0, nil
}

// buildConnectionString constructs the SQLite connection string with pragmas
func buildConnectionString(options *DatabaseOptions) string {
	var pragmas []string

	if options.WALMode {
		pragmas = append(pragmas, "journal_mode=WAL")
	}

	if options.ForeignKeys {
		pragmas = append(pragmas, "foreign_keys=ON")
	}

	if options.BusyTimeout > 0 {
		pragmas = append(pragmas, fmt.Sprintf("busy_timeout=%d", int(options.BusyTimeout.Milliseconds())))
	}

	// Add performance optimizations
	pragmas = append(pragmas,
		"synchronous=NORMAL",
		"cache_size=10000",
		"temp_store=memory",
		"mmap_size=268435456", // 256MB mmap
	)

	connStr := options.Path
	if len(pragmas) > 0 {
		connStr += "?" + strings.Join(pragmas, "&")
	}

	return connStr
}

// ensureDirectory creates the directory for the database file if it doesn't exist
func ensureDirectory(dbPath string) error {
	dir := filepath.Dir(dbPath)
	if dir == "." || dir == "" {
		return nil // Current directory, no need to create
	}

	return os.MkdirAll(dir, 0755)
}
