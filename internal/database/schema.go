package database

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"

	"github.com/jchantrell/exiledb/internal/dat"
	"github.com/jchantrell/exiledb/internal/utils"
)

// SchemaProgressCallback is called during schema creation to report progress
type SchemaProgressCallback func(current int, total int, description string)

// DDLManager handles schema creation with bulk DDL execution
type DDLManager struct {
	db             *Database
	maxConcurrency int
}

// NewDDLManager creates a new DDL manager
func NewDDLManager(db *Database) *DDLManager {
	return &DDLManager{
		db:             db,
		maxConcurrency: runtime.NumCPU(),
	}
}

// GenerateTableDDL generates CREATE TABLE SQL for a given table schema
func (dm *DDLManager) GenerateTableDDL(table *dat.TableSchema) (string, error) {
	if table == nil {
		return "", fmt.Errorf("table schema cannot be nil")
	}

	if table.Name == "" {
		return "", fmt.Errorf("table name cannot be empty")
	}

	tableName := utils.ToSnakeCase(table.Name)

	var columns []string
	var foreignKeys []string

	// Add standard columns first
	columns = append(columns, "_index INTEGER")
	columns = append(columns, "_language TEXT NOT NULL")

	// Add schema-defined columns
	for i, column := range table.Columns {
		if column.Name == nil {
			// Skip unnamed columns
			continue
		}

		columnName := utils.ToSnakeCase(*column.Name)
		columnDDL, fkDDL, err := dm.generateColumnDDL(&column, columnName)
		if err != nil {
			return "", fmt.Errorf("generating column %d (%s): %w", i, columnName, err)
		}

		// Only add non-empty column DDL (foreign key arrays return empty DDL)
		if columnDDL != "" {
			columns = append(columns, columnDDL)
		}

		if fkDDL != "" {
			foreignKeys = append(foreignKeys, fkDDL)
		}
	}

	// Add primary key
	columns = append(columns, "PRIMARY KEY (_language, _index)")

	// Add foreign key constraints
	columns = append(columns, foreignKeys...)

	// Build the CREATE TABLE statement
	ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n    %s\n)",
		quoteSQLIdentifier(tableName),
		strings.Join(columns, ",\n    "))

	return ddl, nil
}

// generateColumnDDL generates the DDL for a single column with version-aware foreign key handling
func (dm *DDLManager) generateColumnDDL(column *dat.TableColumn, columnName string) (string, string, error) {
	if column.Array {
		// Array columns are stored as JSON text unless they have foreign key references
		if column.References != nil {
			// This is a foreign key array - we'll store this info for junction table generation
			// Return empty column DDL as the data will be stored in junction table
			return "", "", nil
		} else {
			// Simple array stored as JSON
			return fmt.Sprintf("%s TEXT", quoteSQLIdentifier(columnName)), "", nil
		}
	}

	// Generate the base column definition
	baseType, err := dm.mapDATTypeToSQL(column.Type)
	if err != nil {
		return "", "", fmt.Errorf("mapping type %s: %w", column.Type, err)
	}

	columnDDL := fmt.Sprintf("%s %s", quoteSQLIdentifier(columnName), baseType)

	// Generate foreign key constraint if this column references another table
	var foreignKeyDDL string
	if column.References != nil && !column.Array {
		fkDDL, err := dm.generateForeignKeyDDL(columnName, column.References)
		if err != nil {
			return "", "", fmt.Errorf("generating foreign key for %s: %w", columnName, err)
		}
		foreignKeyDDL = fkDDL
	}

	return columnDDL, foreignKeyDDL, nil
}

// mapDATTypeToSQL maps DAT field types to SQLite types
func (dm *DDLManager) mapDATTypeToSQL(fieldType dat.FieldType) (string, error) {
	switch fieldType {
	case dat.TypeBool:
		return "INTEGER", nil // SQLite stores booleans as integers
	case dat.TypeString:
		return "TEXT", nil
	case dat.TypeInt16, dat.TypeInt32, dat.TypeInt64:
		return "INTEGER", nil
	case dat.TypeUint16, dat.TypeUint32, dat.TypeUint64:
		return "INTEGER", nil
	case dat.TypeFloat32, dat.TypeFloat64:
		return "REAL", nil
	case dat.TypeRow, dat.TypeForeignRow, dat.TypeEnumRow:
		return "INTEGER", nil // Row references are integer indices
	case dat.TypeArray:
		return "TEXT", nil // Arrays stored as JSON text
	default:
		return "", fmt.Errorf("unsupported field type: %s", fieldType)
	}
}

// generateForeignKeyDDL generates a foreign key constraint with version-aware table names
func (dm *DDLManager) generateForeignKeyDDL(columnName string, ref *dat.ColumnReference) (string, error) {
	if ref == nil {
		return "", fmt.Errorf("column reference cannot be nil")
	}

	if ref.Table == "" {
		return "", fmt.Errorf("referenced table cannot be empty")
	}

	// Generate unified referenced table name
	referencedTable := utils.ToSnakeCase(ref.Table)
	referencedColumn := "_index" // Default to _index column

	if ref.Column != nil && *ref.Column != "" {
		referencedColumn = utils.ToSnakeCase(*ref.Column)
	}

	// Foreign keys in ExileDB include the language dimension
	fkDDL := fmt.Sprintf("FOREIGN KEY (_language, %s) REFERENCES %s(_language, %s)",
		quoteSQLIdentifier(columnName), quoteSQLIdentifier(referencedTable), quoteSQLIdentifier(referencedColumn))

	return fkDDL, nil
}

// generateJunctionTableDDL generates CREATE TABLE SQL for a junction table with version-aware table names
func (dm *DDLManager) generateJunctionTableDDL(tableName, columnName string, ref *dat.ColumnReference) (string, error) {
	if ref == nil {
		return "", fmt.Errorf("column reference cannot be nil")
	}

	if ref.Table == "" {
		return "", fmt.Errorf("referenced table cannot be empty")
	}

	// Generate junction table name: {main_table}_{column}_junction
	junctionTableName := fmt.Sprintf("%s_%s_junction", tableName, columnName)

	// Generate unified referenced table name
	referencedTable := utils.ToSnakeCase(ref.Table)
	referencedColumn := "_index" // Default to _index column

	if ref.Column != nil && *ref.Column != "" {
		referencedColumn = utils.ToSnakeCase(*ref.Column)
	}

	// Build junction table DDL with composite foreign key pattern
	ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    _language TEXT NOT NULL,
    _parent_index INTEGER NOT NULL,
    _array_index INTEGER NOT NULL,
    value INTEGER,
    FOREIGN KEY (_language, _parent_index) 
      REFERENCES %s(_language, _index),
    FOREIGN KEY (_language, value) 
      REFERENCES %s(_language, %s),
    UNIQUE(_language, _parent_index, _array_index)
)`, quoteSQLIdentifier(junctionTableName), quoteSQLIdentifier(tableName), quoteSQLIdentifier(referencedTable), quoteSQLIdentifier(referencedColumn))

	return ddl, nil
}

// CreateTableSchema creates the complete database schema for a table.
func (dm *DDLManager) CreateTableSchema(ctx context.Context, table *dat.TableSchema) error {
	if dm.db == nil {
		return fmt.Errorf("database cannot be nil")
	}

	if table == nil {
		return fmt.Errorf("table schema cannot be nil")
	}

	tableName := utils.ToSnakeCase(table.Name)

	// Generate and execute main table DDL
	tableDDL, err := dm.GenerateTableDDL(table)
	if err != nil {
		return fmt.Errorf("generating table DDL for %s: %w", tableName, err)
	}

	if _, err := dm.db.Exec(ctx, tableDDL); err != nil {
		return fmt.Errorf("creating table %s: %w", tableName, err)
	}

	// Generate and execute junction tables for foreign key arrays
	junctionTableCount := 0
	for _, column := range table.Columns {
		if column.Name == nil || !column.Array || column.References == nil {
			continue
		}

		columnName := utils.ToSnakeCase(*column.Name)

		// Create junction table for this column (no filtering by referenced table)
		junctionDDL, err := dm.generateJunctionTableDDL(tableName, columnName, column.References)
		if err != nil {
			return fmt.Errorf("generating junction table DDL for %s.%s: %w", tableName, columnName, err)
		}

		if _, err := dm.db.Exec(ctx, junctionDDL); err != nil {
			return fmt.Errorf("creating junction table for %s.%s: %w", tableName, columnName, err)
		}

		junctionTableCount++
		slog.Debug("Created junction table",
			"main_table", tableName,
			"column", columnName,
			"referenced_table", column.References.Table,
			"junction_table", fmt.Sprintf("%s_%s_junction", tableName, columnName))
	}

	slog.Info("Created table schema",
		"table", tableName,
		"columns", len(table.Columns),
		"junction_tables_created", junctionTableCount)
	return nil
}

// DDLRequest represents a request to generate and execute DDL
type DDLRequest struct {
	Type        string // "table" or "junction"
	DDL         string
	TableName   string
	Description string
}

// CreateSchemas creates all schemas using bulk execution for optimal performance
func (dm *DDLManager) CreateSchemas(ctx context.Context, tables []dat.TableSchema, progressCallback SchemaProgressCallback) error {
	if len(tables) == 0 {
		return nil
	}

	// Phase 1: Generate all DDL in parallel (CPU-bound operation)
	ddlRequests, err := dm.generateAllDDLParallel(tables)
	if err != nil {
		return fmt.Errorf("generating DDL: %w", err)
	}

	// Phase 2: Execute DDL in controlled batches to avoid SQLite contention
	if err := dm.executeDDLBulk(ctx, ddlRequests, progressCallback); err != nil {
		return fmt.Errorf("executing DDL: %w", err)
	}

	return nil
}

// generateAllDDLParallel generates all DDL statements in parallel
func (dm *DDLManager) generateAllDDLParallel(tables []dat.TableSchema) ([]DDLRequest, error) {
	// Channel for DDL generation work
	type ddlWork struct {
		table dat.TableSchema
		index int
	}

	workChan := make(chan ddlWork, len(tables))
	resultsChan := make(chan []DDLRequest, len(tables))
	errorsChan := make(chan error, len(tables))

	// Send all work
	for i, table := range tables {
		workChan <- ddlWork{table: table, index: i}
	}
	close(workChan)

	// Start workers
	var wg sync.WaitGroup
	numWorkers := min(dm.maxConcurrency, len(tables))

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for work := range workChan {
				ddlRequests, err := dm.generateTableDDLRequests(work.table)
				if err != nil {
					errorsChan <- fmt.Errorf("generating DDL for table %s: %w", work.table.Name, err)
					return
				}
				resultsChan <- ddlRequests
			}
		}()
	}

	// Wait for completion
	go func() {
		wg.Wait()
		close(resultsChan)
		close(errorsChan)
	}()

	// Collect results
	var allDDL []DDLRequest
	var errors []error

	for {
		select {
		case ddlRequests, ok := <-resultsChan:
			if !ok {
				resultsChan = nil
			} else {
				allDDL = append(allDDL, ddlRequests...)
			}
		case err, ok := <-errorsChan:
			if !ok {
				errorsChan = nil
			} else {
				errors = append(errors, err)
			}
		}

		if resultsChan == nil && errorsChan == nil {
			break
		}
	}

	if len(errors) > 0 {
		return nil, fmt.Errorf("DDL generation failed: %v", errors[0])
	}

	return allDDL, nil
}

// generateTableDDLRequests generates all DDL requests for a single table
func (dm *DDLManager) generateTableDDLRequests(table dat.TableSchema) ([]DDLRequest, error) {
	var requests []DDLRequest

	tableName := utils.ToSnakeCase(table.Name)

	// Generate main table DDL
	tableDDL, err := dm.GenerateTableDDL(&table)
	if err != nil {
		return nil, fmt.Errorf("generating table DDL: %w", err)
	}

	requests = append(requests, DDLRequest{
		Type:        "table",
		DDL:         tableDDL,
		TableName:   tableName,
		Description: table.Name,
	})

	// Generate junction table DDL
	for _, column := range table.Columns {
		if column.Name == nil || !column.Array || column.References == nil {
			continue
		}

		columnName := utils.ToSnakeCase(*column.Name)
		junctionDDL, err := dm.generateJunctionTableDDL(tableName, columnName, column.References)
		if err != nil {
			return nil, fmt.Errorf("generating junction table DDL for %s.%s: %w", tableName, columnName, err)
		}

		junctionTableName := fmt.Sprintf("%s_%s_junction", tableName, columnName)
		requests = append(requests, DDLRequest{
			Type:        "junction",
			DDL:         junctionDDL,
			TableName:   junctionTableName,
			Description: fmt.Sprintf("%s.%s", table.Name, *column.Name),
		})
	}

	return requests, nil
}

// executeDDLBulk executes DDL statements in bulk transactions
func (dm *DDLManager) executeDDLBulk(ctx context.Context, ddlRequests []DDLRequest, progressCallback SchemaProgressCallback) error {
	// Separate main tables and junction tables to ensure main tables are created first
	var mainTableRequests []DDLRequest
	var junctionTableRequests []DDLRequest

	for _, req := range ddlRequests {
		if req.Type == "table" {
			mainTableRequests = append(mainTableRequests, req)
		} else {
			junctionTableRequests = append(junctionTableRequests, req)
		}
	}

	totalTables := len(mainTableRequests) + len(junctionTableRequests)
	currentProgress := 0

	// Execute main tables in single transaction
	if err := dm.executeDDLTransaction(ctx, mainTableRequests, "main tables", progressCallback, &currentProgress, totalTables); err != nil {
		return fmt.Errorf("executing main tables: %w", err)
	}

	// Execute junction tables in single transaction
	if err := dm.executeDDLTransaction(ctx, junctionTableRequests, "junction tables", progressCallback, &currentProgress, totalTables); err != nil {
		return fmt.Errorf("executing junction tables: %w", err)
	}

	return nil
}

// executeDDLTransaction executes DDL statements in a single transaction with progress reporting
func (dm *DDLManager) executeDDLTransaction(ctx context.Context, ddlRequests []DDLRequest, description string, progressCallback SchemaProgressCallback, currentProgress *int, totalTables int) error {
	if len(ddlRequests) == 0 {
		return nil
	}

	// Begin transaction for bulk DDL execution
	tx, err := dm.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction for %s: %w", description, err)
	}
	defer tx.Rollback() // Safe to call even after commit

	// Execute all DDL statements in the transaction
	for _, req := range ddlRequests {
		if _, err := tx.ExecContext(ctx, req.DDL); err != nil {
			return fmt.Errorf("executing DDL for %s in %s: %w", req.TableName, description, err)
		}

		// Update progress after each table schema is created
		if progressCallback != nil {
			*currentProgress++
			progressCallback(*currentProgress, totalTables, req.Description)
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing DDL transaction for %s: %w", description, err)
	}

	return nil
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// quoteSQLIdentifier quotes SQL identifiers to prevent conflicts with reserved words
func quoteSQLIdentifier(identifier string) string {
	// In SQLite, identifiers can be quoted with double quotes
	return fmt.Sprintf(`"%s"`, identifier)
}
