package database

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/jchantrell/exiledb/internal/dat"
	"github.com/jchantrell/exiledb/internal/poe"
)

// SchemaProgressCallback is called during schema creation to report progress
type SchemaProgressCallback func(current int, total int, description string)

// DDLManager handles schema creation with bulk DDL execution
type DDLManager struct {
	db *Database
}

// NewDDLManager creates a new DDL manager
func NewDDLManager(db *Database) *DDLManager {
	return &DDLManager{db: db}
}

// columnNames is the single owner of the schema column naming convention:
// unnamed columns become "unknownN" in SQL and "UnknownN" in parsed row data,
// named columns use the snake_cased schema name in SQL and the schema name as
// the parser field. Both DDL generation and the insert plan derive names here.
func columnNames(col *dat.TableColumn, i int) (sqlName, fieldName string) {
	if col.Name == nil {
		return fmt.Sprintf("unknown%d", i), fmt.Sprintf("Unknown%d", i)
	}
	return poe.ToSnakeCase(*col.Name), *col.Name
}

// GenerateTableDDL generates CREATE TABLE SQL for a given table schema
func (dm *DDLManager) GenerateTableDDL(table *dat.TableSchema) (string, error) {
	if table == nil {
		return "", fmt.Errorf("table schema cannot be nil")
	}

	if table.Name == "" {
		return "", fmt.Errorf("table name cannot be empty")
	}

	tableName := poe.ToSnakeCase(table.Name)
	if err := validateIdentifier(tableName); err != nil {
		return "", fmt.Errorf("table %s: %w", table.Name, err)
	}

	var columns []string
	var foreignKeys []string

	// Add standard columns first
	columns = append(columns, "_index INTEGER")
	columns = append(columns, "_language TEXT NOT NULL")

	// Add schema-defined columns
	for i, column := range table.Columns {
		columnName, _ := columnNames(&column, i)
		if err := validateIdentifier(columnName); err != nil {
			return "", fmt.Errorf("table %s column %d: %w", table.Name, i, err)
		}

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
			return "", "", err
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
		return "", fmt.Errorf("nil reference")
	}

	if ref.Table == "" {
		return "", fmt.Errorf("empty table")
	}

	// Generate unified referenced table name
	referencedTable := poe.ToSnakeCase(ref.Table)
	referencedColumn := "_index" // Default to _index column

	if ref.Column != nil && *ref.Column != "" {
		referencedColumn = poe.ToSnakeCase(*ref.Column)
	}

	// Foreign keys in ExileDB include the language dimension
	fkDDL := fmt.Sprintf("FOREIGN KEY (_language, %s) REFERENCES %s(_language, %s)",
		quoteSQLIdentifier(columnName), quoteSQLIdentifier(referencedTable), quoteSQLIdentifier(referencedColumn))

	return fkDDL, nil
}

// generateJunctionTableDDL generates CREATE TABLE SQL for a junction table with version-aware table names
func (dm *DDLManager) generateJunctionTableDDL(tableName, columnName string, ref *dat.ColumnReference) (string, error) {
	if ref == nil {
		return "", fmt.Errorf("nil reference")
	}

	if ref.Table == "" {
		return "", fmt.Errorf("empty table")
	}

	// Generate junction table name: {main_table}_{column}_junction
	junctionTableName := fmt.Sprintf("%s_%s_junction", tableName, columnName)

	// Generate unified referenced table name
	referencedTable := poe.ToSnakeCase(ref.Table)
	referencedColumn := "_index" // Default to _index column

	if ref.Column != nil && *ref.Column != "" {
		referencedColumn = poe.ToSnakeCase(*ref.Column)
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

// DDLRequest represents a request to execute a generated DDL statement
type DDLRequest struct {
	DDL         string
	TableName   string
	Description string
}

// CreateSchemas creates all schemas using bulk execution for optimal performance
func (dm *DDLManager) CreateSchemas(ctx context.Context, tables []dat.TableSchema, progressCallback SchemaProgressCallback) error {
	if len(tables) == 0 {
		return nil
	}

	mainRequests, junctionRequests, err := dm.generateAllDDL(tables)
	if err != nil {
		return fmt.Errorf("generating DDL: %w", err)
	}

	if err := dm.executeDDLBulk(ctx, mainRequests, junctionRequests, progressCallback); err != nil {
		return fmt.Errorf("executing DDL: %w", err)
	}

	return nil
}

// generateAllDDL generates the main table and junction table DDL for all tables
func (dm *DDLManager) generateAllDDL(tables []dat.TableSchema) ([]DDLRequest, []DDLRequest, error) {
	var mainRequests []DDLRequest
	var junctionRequests []DDLRequest

	for _, table := range tables {
		main, junctions, err := dm.generateTableDDLRequests(table)
		if err != nil {
			return nil, nil, fmt.Errorf("generating DDL for table %s: %w", table.Name, err)
		}
		mainRequests = append(mainRequests, main)
		junctionRequests = append(junctionRequests, junctions...)
	}

	return mainRequests, junctionRequests, nil
}

// generateTableDDLRequests generates the main table request and any junction
// table requests for a single table
func (dm *DDLManager) generateTableDDLRequests(table dat.TableSchema) (DDLRequest, []DDLRequest, error) {
	tableName := poe.ToSnakeCase(table.Name)

	tableDDL, err := dm.GenerateTableDDL(&table)
	if err != nil {
		return DDLRequest{}, nil, fmt.Errorf("generating table DDL: %w", err)
	}

	main := DDLRequest{
		DDL:         tableDDL,
		TableName:   tableName,
		Description: table.Name,
	}

	var junctions []DDLRequest
	for i := range table.Columns {
		column := &table.Columns[i]
		if column.Name == nil || !column.Array || column.References == nil {
			continue
		}

		columnName, _ := columnNames(column, i)
		junctionDDL, err := dm.generateJunctionTableDDL(tableName, columnName, column.References)
		if err != nil {
			return DDLRequest{}, nil, err
		}

		junctions = append(junctions, DDLRequest{
			DDL:         junctionDDL,
			TableName:   fmt.Sprintf("%s_%s_junction", tableName, columnName),
			Description: fmt.Sprintf("%s.%s", table.Name, *column.Name),
		})
	}

	return main, junctions, nil
}

// executeDDLBulk executes DDL statements in bulk transactions, main tables
// first so junction table foreign keys have their targets
func (dm *DDLManager) executeDDLBulk(ctx context.Context, mainRequests, junctionRequests []DDLRequest, progressCallback SchemaProgressCallback) error {
	totalTables := len(mainRequests) + len(junctionRequests)
	created := 0
	report := func(description string) {
		created++
		if progressCallback != nil {
			progressCallback(created, totalTables, description)
		}
	}

	// Execute main tables in single transaction
	if err := dm.executeDDLTransaction(ctx, mainRequests, "main tables", report); err != nil {
		return fmt.Errorf("executing main tables: %w", err)
	}

	// Execute junction tables in single transaction
	if err := dm.executeDDLTransaction(ctx, junctionRequests, "junction tables", report); err != nil {
		return fmt.Errorf("executing junction tables: %w", err)
	}

	return nil
}

// executeDDLTransaction executes DDL statements in a single transaction with progress reporting
func (dm *DDLManager) executeDDLTransaction(ctx context.Context, ddlRequests []DDLRequest, description string, report func(description string)) error {
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

		report(req.Description)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing DDL transaction for %s: %w", description, err)
	}

	return nil
}

// quoteSQLIdentifier quotes SQL identifiers to prevent conflicts with
// reserved words. Embedded quotes are doubled: identifiers originate from
// the community schema JSON, which is downloaded at runtime and must not be
// able to break out of the quoting.
func quoteSQLIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

// validIdentifier matches the only characters table/column names may
// contain; anything else in the remote schema is rejected before it reaches
// DDL generation.
var validIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func validateIdentifier(identifier string) error {
	if !validIdentifier.MatchString(identifier) {
		return fmt.Errorf("invalid SQL identifier %q", identifier)
	}
	return nil
}
