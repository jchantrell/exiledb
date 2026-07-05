package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"strings"

	"github.com/jchantrell/exiledb/internal/dat"
)

// BulkInserter handles efficient batch insertion of DAT file data
type BulkInserter struct {
	db        *Database
	batchSize int
}

// BulkInsertOptions configures bulk insertion behavior
type BulkInsertOptions struct {
	// BatchSize determines how many rows to insert between progress reports
	BatchSize int
}

// DefaultBulkInsertOptions returns sensible defaults for bulk insertion
func DefaultBulkInsertOptions() *BulkInsertOptions {
	return &BulkInsertOptions{
		BatchSize: 1000,
	}
}

// NewBulkInserter creates a new bulk inserter with the given database and options
func NewBulkInserter(db *Database, options *BulkInsertOptions) *BulkInserter {
	if options == nil {
		options = DefaultBulkInsertOptions()
	}

	return &BulkInserter{
		db:        db,
		batchSize: options.BatchSize,
	}
}

// TableData represents parsed data for a single table
type TableData struct {
	// Schema is the table schema from the community schema
	Schema *dat.TableSchema

	// Rows contains the parsed row data
	Rows []RowData

	// Metadata about the data
	Language string
}

// RowData represents a single row of data with column values
type RowData struct {
	Index  int            // Row index from DAT file
	Values map[string]any // Column name -> value mapping
}

// colBinding binds one SQL column to its parser field and value conversion.
type colBinding struct {
	sqlName string
	field   string                 // parser field name ("Unknown3" or *col.Name)
	process func(any) (any, error) // bound per column type/references at plan time
}

// junctionBinding binds one foreign key array column to its junction table.
type junctionBinding struct {
	sqlName   string
	field     string // parser field name
	insertSQL string
}

// insertPlan is the schema-to-SQL mapping for one table, computed once per
// table instead of being re-derived per column per row.
type insertPlan struct {
	tableName string
	insertSQL string
	cols      []colBinding
	junctions []junctionBinding
}

// buildInsertPlan derives the insert statement and column bindings for a
// table from the same table plan that DDL generation uses.
func buildInsertPlan(schema *dat.TableSchema) (*insertPlan, error) {
	plan, err := newTablePlan(schema)
	if err != nil {
		return nil, err
	}

	quotedColumns := []string{quoteSQLIdentifier(colIndex), quoteSQLIdentifier(colLanguage)}
	placeholders := []string{"?", "?"}

	cols := make([]colBinding, 0, len(plan.columns))
	for _, col := range plan.columns {
		cols = append(cols, colBinding{
			sqlName: col.sqlName,
			field:   col.field,
			process: valueProcessor(col.column),
		})
		quotedColumns = append(quotedColumns, quoteSQLIdentifier(col.sqlName))
		placeholders = append(placeholders, "?")
	}

	junctions := make([]junctionBinding, 0, len(plan.junctions))
	for _, junction := range plan.junctions {
		junctions = append(junctions, junctionBinding{
			sqlName: junction.sqlName,
			field:   junction.field,
			insertSQL: fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)",
				quoteSQLIdentifier(junction.tableName),
				quoteSQLIdentifier(colLanguage),
				quoteSQLIdentifier(colParentIndex),
				quoteSQLIdentifier(colArrayIndex),
				quoteSQLIdentifier(colValue)),
		})
	}

	return &insertPlan{
		tableName: plan.sqlName,
		insertSQL: fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			quoteSQLIdentifier(plan.sqlName),
			strings.Join(quotedColumns, ", "),
			strings.Join(placeholders, ", ")),
		cols:      cols,
		junctions: junctions,
	}, nil
}

// valueProcessor selects the value conversion for a column at plan time.
func valueProcessor(column *dat.TableColumn) func(any) (any, error) {
	switch {
	case column.Array && column.References == nil:
		return processArrayValue
	case column.References != nil:
		return processReferenceValue
	default:
		fieldType := column.Type
		return func(value any) (any, error) {
			return processScalarValue(value, fieldType)
		}
	}
}

// InsertTableData loads a table in a single transaction with statements
// prepared once up front; a mid-table failure rolls the whole table back.
func (bi *BulkInserter) InsertTableData(ctx context.Context, tableData *TableData) error {
	if tableData == nil {
		return fmt.Errorf("table data cannot be nil")
	}

	if tableData.Schema == nil {
		return fmt.Errorf("table schema cannot be nil")
	}

	if len(tableData.Rows) == 0 {
		slog.Debug("No rows to insert", "table", tableData.Schema.Name)
		return nil
	}

	plan, err := buildInsertPlan(tableData.Schema)
	if err != nil {
		return fmt.Errorf("planning insert for %s: %w", tableData.Schema.Name, err)
	}
	tableName := plan.tableName

	tx, err := bi.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback() // Safe to call even after commit

	stmt, err := tx.PrepareContext(ctx, plan.insertSQL)
	if err != nil {
		return fmt.Errorf("preparing insert statement for %s: %w", tableName, err)
	}
	defer stmt.Close()

	junctionStmts := make([]*sql.Stmt, len(plan.junctions))
	for i, junction := range plan.junctions {
		junctionStmt, err := tx.PrepareContext(ctx, junction.insertSQL)
		if err != nil {
			return fmt.Errorf("preparing junction table statement for %s.%s: %w", tableName, junction.sqlName, err)
		}
		defer junctionStmt.Close()
		junctionStmts[i] = junctionStmt
	}

	for n, row := range tableData.Rows {
		if err := insertRow(ctx, plan, stmt, junctionStmts, tableData, &row); err != nil {
			return fmt.Errorf("inserting row %d for table %s: %w", row.Index, tableName, err)
		}

		if bi.batchSize > 0 && (n+1)%bi.batchSize == 0 {
			slog.Debug("Insert progress", "table", tableName, "rows", n+1, "total", len(tableData.Rows))
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction for %s: %w", tableName, err)
	}

	return nil
}

// insertRow binds one row's values against the plan and executes the main
// insert plus any junction table inserts.
func insertRow(ctx context.Context, plan *insertPlan, stmt *sql.Stmt, junctionStmts []*sql.Stmt, tableData *TableData, row *RowData) error {
	values := make([]any, 0, len(plan.cols)+2)
	values = append(values, row.Index, tableData.Language)

	for _, col := range plan.cols {
		raw, exists := row.Values[col.field]
		if !exists {
			values = append(values, nil) // NULL for missing columns
			continue
		}

		processed, err := col.process(raw)
		if err != nil {
			return fmt.Errorf("processing value for column %s: %w", col.sqlName, err)
		}
		values = append(values, processed)
	}

	if _, err := stmt.ExecContext(ctx, values...); err != nil {
		return err
	}

	for i := range plan.junctions {
		if err := insertJunctionRows(ctx, junctionStmts[i], &plan.junctions[i], tableData, row); err != nil {
			return err
		}
	}

	return nil
}

// insertJunctionRows inserts one row's array elements into a junction table,
// dropping null references while preserving array indices.
func insertJunctionRows(ctx context.Context, stmt *sql.Stmt, junction *junctionBinding, tableData *TableData, row *RowData) error {
	value, exists := row.Values[junction.field]
	if !exists {
		return nil // No data for this array column
	}

	arrayValues, err := convertToSlice(value)
	if err != nil {
		return fmt.Errorf("converting array value for column %s: %w", junction.sqlName, err)
	}

	for arrayIndex, arrayValue := range arrayValues {
		processed, err := processReferenceValue(arrayValue)
		if err != nil {
			return fmt.Errorf("processing array element at index %d: %w", arrayIndex, err)
		}

		if processed == nil {
			continue // Skip null references
		}

		if _, err := stmt.ExecContext(ctx, tableData.Language, row.Index, arrayIndex, processed); err != nil {
			return fmt.Errorf("inserting junction row for %s[%d]: %w", junction.sqlName, arrayIndex, err)
		}
	}

	return nil
}

// processArrayValue serializes a simple (non foreign key) array as JSON text.
func processArrayValue(value any) (any, error) {
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("serializing array value to JSON: %w", err)
	}
	return string(jsonBytes), nil
}

// processReferenceValue normalizes foreign key reference values. The parser
// emits row references as *uint32 with nil for null sentinels; junction array
// elements arrive already dereferenced by convertToSlice.
func processReferenceValue(value any) (any, error) {
	switch v := value.(type) {
	case *uint32:
		if v == nil {
			return nil, nil // NULL reference
		}
		return int64(*v), nil // Convert to signed integer for SQLite
	case uint32:
		return int64(v), nil
	}

	// Pass through other types
	return value, nil
}

// processScalarValue handles basic scalar value processing
func processScalarValue(value any, fieldType dat.FieldType) (any, error) {
	// Handle boolean conversion to integer
	if fieldType == dat.TypeBool {
		if boolVal, ok := value.(bool); ok {
			if boolVal {
				return int64(1), nil
			}
			return int64(0), nil
		}
	}

	// Convert numeric types to appropriate SQLite types
	switch fieldType {
	case dat.TypeInt16, dat.TypeInt32, dat.TypeInt64:
		// Convert to int64 for SQLite INTEGER
		switch v := value.(type) {
		case int16:
			return int64(v), nil
		case int32:
			return int64(v), nil
		case int64:
			return v, nil
		}
	case dat.TypeUint16, dat.TypeUint32, dat.TypeUint64:
		// Convert unsigned to signed for SQLite
		switch v := value.(type) {
		case uint16:
			return int64(v), nil
		case uint32:
			return int64(v), nil
		case uint64:
			return int64(v), nil
		}
	case dat.TypeFloat32, dat.TypeFloat64:
		// Convert to float64 for SQLite REAL
		switch v := value.(type) {
		case float32:
			return float64(v), nil
		case float64:
			return v, nil
		}
	}

	// Pass through strings and other types as-is
	return value, nil
}

// convertToSlice converts a typed slice to []any, dereferencing pointer
// elements. Nil pointers stay nil so callers can drop them while preserving
// the original array indices.
func convertToSlice(value any) ([]any, error) {
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Slice {
		return nil, fmt.Errorf("unsupported array type: %T", value)
	}

	result := make([]any, rv.Len())
	for i := range result {
		elem := rv.Index(i)
		if elem.Kind() == reflect.Pointer {
			if elem.IsNil() {
				continue
			}
			elem = elem.Elem()
		}
		result[i] = elem.Interface()
	}
	return result, nil
}
