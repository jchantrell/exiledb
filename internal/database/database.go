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

type Database struct {
	db   *sql.DB
	path string
}

// Foreign keys are never enforced: the generated FOREIGN KEY clauses are
// documentation for query tools, and game data references rows in any order,
// so loads run with enforcement off. Use CheckForeignKeys after a load to
// report violations.
type DatabaseOptions struct {
	Path string

	WALMode bool

	BusyTimeout time.Duration
}

func DefaultDatabaseOptions(path string) *DatabaseOptions {
	return &DatabaseOptions{
		Path:        path,
		WALMode:     true,
		BusyTimeout: 30 * time.Second,
	}
}

func NewDatabase(options *DatabaseOptions) (*Database, error) {
	if options == nil {
		return nil, fmt.Errorf("database options cannot be nil")
	}

	if options.Path == "" {
		return nil, fmt.Errorf("database path cannot be empty")
	}

	if err := ensureDirectory(options.Path); err != nil {
		return nil, fmt.Errorf("creating database directory: %w", err)
	}

	connStr := buildConnectionString(options)

	db, err := sql.Open("sqlite3", connStr)
	if err != nil {
		return nil, fmt.Errorf("opening database %s: %w", options.Path, err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("testing database connection: %w", err)
	}

	// These pragmas are not DSN parameters in mattn/go-sqlite3, so they must
	// be executed explicitly.
	for _, pragma := range []string{
		"PRAGMA temp_store = MEMORY",
		"PRAGMA mmap_size = 268435456", // 256MB
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("applying %s: %w", pragma, err)
		}
	}

	// Verify the DSN pragmas actually took effect; a silently ignored
	// parameter (the bug this guards against) is a misconfiguration, not a
	// degraded mode.
	if options.WALMode {
		var mode string
		if err := db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil || !strings.EqualFold(mode, "wal") {
			db.Close()
			return nil, fmt.Errorf("WAL mode requested but journal_mode is %q (err: %v)", mode, err)
		}
	}

	database := &Database{
		db:   db,
		path: options.Path,
	}

	return database, nil
}

type ForeignKeyViolation struct {
	Table  string
	RowID  int64
	Parent string
}

// CheckForeignKeys reports foreign-key violations. Constraints are emitted as
// documentation and never enforced during load, so this is how a load
// surfaces referential problems. Violations whose parent table does not exist
// are skipped: a partial extraction legitimately references tables that were
// never extracted, and flagging every such row is noise, not signal.
func (d *Database) CheckForeignKeys(ctx context.Context) ([]ForeignKeyViolation, error) {
	if d.db == nil {
		return nil, fmt.Errorf("database connection is closed")
	}

	tables, err := d.db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table'")
	if err != nil {
		return nil, fmt.Errorf("listing tables: %w", err)
	}
	existing := make(map[string]bool)
	for tables.Next() {
		var name string
		if err := tables.Scan(&name); err != nil {
			tables.Close()
			return nil, err
		}
		existing[name] = true
	}
	tables.Close()
	if err := tables.Err(); err != nil {
		return nil, err
	}

	rows, err := d.db.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return nil, fmt.Errorf("running foreign_key_check: %w", err)
	}
	defer rows.Close()

	var violations []ForeignKeyViolation
	for rows.Next() {
		var v ForeignKeyViolation
		var rowid sql.NullInt64
		var fkid int64
		if err := rows.Scan(&v.Table, &rowid, &v.Parent, &fkid); err != nil {
			return nil, fmt.Errorf("scanning foreign_key_check row: %w", err)
		}
		if !existing[v.Parent] {
			continue
		}
		v.RowID = rowid.Int64
		violations = append(violations, v)
	}
	return violations, rows.Err()
}

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

func (d *Database) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if d.db == nil {
		return nil, fmt.Errorf("database connection is closed")
	}

	result, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("executing query: %w", err)
	}

	return result, nil
}

func (d *Database) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if d.db == nil {
		return nil, fmt.Errorf("database connection is closed")
	}

	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("executing query: %w", err)
	}

	return rows, nil
}

func (d *Database) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return d.db.QueryRowContext(ctx, query, args...)
}

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

// buildConnectionString constructs the SQLite DSN. mattn/go-sqlite3 only
// recognizes underscore-prefixed parameters; anything else is silently
// dropped by the driver.
func buildConnectionString(options *DatabaseOptions) string {
	params := []string{
		"_foreign_keys=off", // FK clauses are documentation; see DatabaseOptions
		"_synchronous=NORMAL",
		"_cache_size=10000",
	}

	if options.WALMode {
		params = append(params, "_journal_mode=WAL")
	}

	if options.BusyTimeout > 0 {
		params = append(params, fmt.Sprintf("_busy_timeout=%d", int(options.BusyTimeout.Milliseconds())))
	}

	return options.Path + "?" + strings.Join(params, "&")
}

func ensureDirectory(dbPath string) error {
	dir := filepath.Dir(dbPath)
	if dir == "." || dir == "" {
		return nil // Current directory, no need to create
	}

	return os.MkdirAll(dir, 0755)
}
