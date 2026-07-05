package database

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/jchantrell/exiledb/internal/dat"
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

// generateTableDDL renders the CREATE TABLE statement for a plan's main
// table: standard columns, schema columns in plan order, the primary key,
// then any scalar foreign key clauses.
func generateTableDDL(plan *tablePlan) string {
	columns := []string{
		colIndex + " INTEGER",
		colLanguage + " TEXT NOT NULL",
	}
	var foreignKeys []string

	for _, col := range plan.columns {
		columns = append(columns, fmt.Sprintf("%s %s", quoteSQLIdentifier(col.sqlName), col.sqlType))

		// Foreign keys in ExileDB include the language dimension
		if col.refTable != "" {
			foreignKeys = append(foreignKeys, fmt.Sprintf("FOREIGN KEY (%s, %s) REFERENCES %s(%s, %s)",
				colLanguage, quoteSQLIdentifier(col.sqlName),
				quoteSQLIdentifier(col.refTable), colLanguage, quoteSQLIdentifier(col.refColumn)))
		}
	}

	columns = append(columns, fmt.Sprintf("PRIMARY KEY (%s, %s)", colLanguage, colIndex))
	columns = append(columns, foreignKeys...)

	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n    %s\n)",
		quoteSQLIdentifier(plan.sqlName),
		strings.Join(columns, ",\n    "))
}

// generateJunctionTableDDL renders the CREATE TABLE statement for a foreign
// key array's junction table, with composite foreign keys back to the parent
// row and out to the referenced table.
func generateJunctionTableDDL(plan *tablePlan, junction *planJunction) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]s (
    %[2]s TEXT NOT NULL,
    %[3]s INTEGER NOT NULL,
    %[4]s INTEGER NOT NULL,
    %[5]s INTEGER,
    FOREIGN KEY (%[2]s, %[3]s)
      REFERENCES %[6]s(%[2]s, %[7]s),
    FOREIGN KEY (%[2]s, %[5]s)
      REFERENCES %[8]s(%[2]s, %[9]s),
    UNIQUE(%[2]s, %[3]s, %[4]s)
)`,
		quoteSQLIdentifier(junction.tableName),
		colLanguage, colParentIndex, colArrayIndex, colValue,
		quoteSQLIdentifier(plan.sqlName), colIndex,
		quoteSQLIdentifier(junction.refTable), quoteSQLIdentifier(junction.refColumn))
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
	plan, err := newTablePlan(&table)
	if err != nil {
		return DDLRequest{}, nil, fmt.Errorf("generating table DDL: %w", err)
	}

	main := DDLRequest{
		DDL:         generateTableDDL(plan),
		TableName:   plan.sqlName,
		Description: plan.schemaName,
	}

	junctions := make([]DDLRequest, 0, len(plan.junctions))
	for i := range plan.junctions {
		junction := &plan.junctions[i]
		junctions = append(junctions, DDLRequest{
			DDL:         generateJunctionTableDDL(plan, junction),
			TableName:   junction.tableName,
			Description: fmt.Sprintf("%s.%s", plan.schemaName, junction.field),
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
