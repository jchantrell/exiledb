package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jchantrell/exiledb/internal/dat"
	"github.com/jchantrell/exiledb/internal/utils"
)

// BulkInserter handles efficient batch insertion of DAT file data
type BulkInserter struct {
	db                        *Database
	batchSize                 int
	maxRetries                int
	maxJunctionTableArraySize int
	arrayWarningThreshold     int
}

// BulkInsertOptions configures bulk insertion behavior
type BulkInsertOptions struct {
	// BatchSize determines how many rows to insert per transaction
	BatchSize int

	// MaxRetries sets the maximum number of retry attempts for failed operations
	MaxRetries int

	// MaxJunctionTableArraySize limits the maximum array size for junction table creation
	MaxJunctionTableArraySize int

	// ArrayWarningThreshold sets the threshold for logging warnings about large arrays
	ArrayWarningThreshold int
}

// DefaultBulkInsertOptions returns sensible defaults for bulk insertion
func DefaultBulkInsertOptions() *BulkInsertOptions {
	return &BulkInsertOptions{
		BatchSize:                 1000,
		MaxRetries:                3,
		MaxJunctionTableArraySize: 100000, // Temporarily increased to test core functionality
		ArrayWarningThreshold:     5000,   // Warn for extremely large arrays
	}
}

// NewBulkInserter creates a new bulk inserter with the given database and options
func NewBulkInserter(db *Database, options *BulkInsertOptions) *BulkInserter {
	if options == nil {
		options = DefaultBulkInsertOptions()
	}

	return &BulkInserter{
		db:                        db,
		batchSize:                 options.BatchSize,
		maxRetries:                options.MaxRetries,
		maxJunctionTableArraySize: options.MaxJunctionTableArraySize,
		arrayWarningThreshold:     options.ArrayWarningThreshold,
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
	Index  int                    // Row index from DAT file
	Values map[string]interface{} // Column name -> value mapping
}

// InsertTableData performs bulk insertion of table data with transaction batching
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

	tableName := utils.ToSnakeCase(tableData.Schema.Name)

	// Generate SQL for main table insertion
	insertSQL, columnOrder, err := bi.generateInsertSQL(tableData.Schema)
	if err != nil {
		return fmt.Errorf("generating insert SQL for %s: %w", tableName, err)
	}

	for i := 0; i < len(tableData.Rows); i += bi.batchSize {
		end := i + bi.batchSize
		if end > len(tableData.Rows) {
			end = len(tableData.Rows)
		}

		batch := tableData.Rows[i:end]

		if err := bi.insertBatch(ctx, insertSQL, columnOrder, tableData, batch); err != nil {
			return fmt.Errorf("inserting batch %d-%d for table %s: %w", i, end-1, tableName, err)
		}

	}

	return nil
}

// generateInsertSQL creates the INSERT SQL statement and column ordering
func (bi *BulkInserter) generateInsertSQL(schema *dat.TableSchema) (string, []string, error) {
	tableName := utils.ToSnakeCase(schema.Name)

	// Keep track of both unquoted (for value mapping) and quoted (for SQL) column names
	unquotedColumns := []string{"_index", "_language"}
	quotedColumns := []string{quoteSQLIdentifier("_index"), quoteSQLIdentifier("_language")}
	placeholders := []string{"?", "?"}

	// Add schema-defined columns (excluding arrays with foreign keys)
	for _, column := range schema.Columns {
		if column.Name == nil {
			continue
		}

		// Skip array columns with foreign keys (they go to junction tables)
		if column.Array && column.References != nil {
			continue
		}

		columnName := utils.ToSnakeCase(*column.Name)
		unquotedColumns = append(unquotedColumns, columnName)
		quotedColumns = append(quotedColumns, quoteSQLIdentifier(columnName))
		placeholders = append(placeholders, "?")
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quoteSQLIdentifier(tableName),
		strings.Join(quotedColumns, ", "),
		strings.Join(placeholders, ", "))

	return insertSQL, unquotedColumns, nil
}

// insertBatch inserts a single batch of rows within a transaction
func (bi *BulkInserter) insertBatch(ctx context.Context, insertSQL string, columnOrder []string, tableData *TableData, batch []RowData) error {
	// Start transaction
	tx, err := bi.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback() // Safe to call even after commit

	// Prepare statement
	stmt, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		return fmt.Errorf("preparing insert statement: %w", err)
	}
	defer stmt.Close()

	// Insert each row in the batch
	for _, row := range batch {
		values, err := bi.buildRowValues(columnOrder, tableData, &row)
		if err != nil {
			return fmt.Errorf("building values for row %d: %w", row.Index, err)
		}

		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("inserting row %d: %w", row.Index, err)
		}

		// Insert junction table data for foreign key arrays
		if err := bi.insertJunctionTableData(ctx, tx, tableData, &row); err != nil {
			return fmt.Errorf("inserting junction data for row %d: %w", row.Index, err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

// buildRowValues constructs the ordered parameter values for a row insertion
func (bi *BulkInserter) buildRowValues(columnOrder []string, tableData *TableData, row *RowData) ([]interface{}, error) {
	values := make([]interface{}, len(columnOrder))

	for i, columnName := range columnOrder {
		switch columnName {
		case "_index":
			values[i] = row.Index
		case "_language":
			values[i] = tableData.Language
		default:
			// Get value from row data - need to find the original field name from snake_case column name
			var originalFieldName string
			for j := range tableData.Schema.Columns {
				if tableData.Schema.Columns[j].Name != nil && utils.ToSnakeCase(*tableData.Schema.Columns[j].Name) == columnName {
					originalFieldName = *tableData.Schema.Columns[j].Name
					break
				}
			}

			if originalFieldName != "" {
				if value, exists := row.Values[originalFieldName]; exists {
					processedValue, err := bi.processColumnValue(columnName, value, tableData.Schema)
					if err != nil {
						return nil, fmt.Errorf("processing value for column %s: %w", columnName, err)
					}
					values[i] = processedValue
				} else {
					values[i] = nil // NULL for missing columns
				}
			} else {
				values[i] = nil // NULL for unknown columns
			}
		}
	}

	return values, nil
}

// processColumnValue processes a column value according to its type and constraints
func (bi *BulkInserter) processColumnValue(columnName string, value interface{}, schema *dat.TableSchema) (interface{}, error) {
	// Find column definition
	var column *dat.TableColumn
	for i := range schema.Columns {
		if schema.Columns[i].Name != nil && utils.ToSnakeCase(*schema.Columns[i].Name) == columnName {
			column = &schema.Columns[i]
			break
		}
	}

	if column == nil {
		// Column not found in schema, pass through as-is
		return value, nil
	}

	// Handle array columns (stored as JSON)
	if column.Array && column.References == nil {
		// Simple array - serialize as JSON
		jsonBytes, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("serializing array value to JSON: %w", err)
		}
		return string(jsonBytes), nil
	}

	// Handle foreign key references (null handling with sentinel values)
	if column.References != nil {
		return bi.processReferenceValue(value)
	}

	// Handle other data types
	return bi.processScalarValue(value, column.Type)
}

// processReferenceValue handles foreign key reference values with null sentinel handling
func (bi *BulkInserter) processReferenceValue(value interface{}) (interface{}, error) {
	// Handle null references (0xfefe_fefe sentinel in DAT files)
	if uintVal, ok := value.(uint32); ok {
		if uintVal == 0xfefefefe {
			return nil, nil // NULL reference
		}
		return int64(uintVal), nil // Convert to signed integer for SQLite
	}

	if intVal, ok := value.(int32); ok {
		if uint32(intVal) == 0xfefefefe {
			return nil, nil // NULL reference
		}
		return int64(intVal), nil
	}

	// Pass through other types
	return value, nil
}

// processScalarValue handles basic scalar value processing
func (bi *BulkInserter) processScalarValue(value interface{}, fieldType dat.FieldType) (interface{}, error) {
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

// insertJunctionTableData inserts data for foreign key arrays into junction tables
func (bi *BulkInserter) insertJunctionTableData(ctx context.Context, tx *sql.Tx, tableData *TableData, row *RowData) error {
	tableName := utils.ToSnakeCase(tableData.Schema.Name)

	for _, column := range tableData.Schema.Columns {
		// Only process foreign key arrays
		if column.Name == nil || !column.Array || column.References == nil {
			continue
		}

		columnName := utils.ToSnakeCase(*column.Name)
		junctionTableName := fmt.Sprintf("%s_%s_junction", tableName, columnName)

		// Get the array value for this column using the original field name
		value, exists := row.Values[*column.Name]
		if !exists {
			continue // No data for this array column
		}

		// Convert to slice for processing
		arrayValues, err := bi.convertToSlice(value)
		if err != nil {
			return fmt.Errorf("converting array value for column %s: %w", columnName, err)
		}

		// Insert each array element into the junction table
		junctionSQL := fmt.Sprintf(
			"INSERT INTO %s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)",
			quoteSQLIdentifier(junctionTableName),
			quoteSQLIdentifier("_language"),
			quoteSQLIdentifier("_parent_index"),
			quoteSQLIdentifier("_array_index"),
			quoteSQLIdentifier("value"))

		junctionStmt, err := tx.PrepareContext(ctx, junctionSQL)
		if err != nil {
			return fmt.Errorf("preparing junction table statement: %w", err)
		}
		defer junctionStmt.Close()

		for arrayIndex, arrayValue := range arrayValues {
			// Process the reference value (handle null sentinels)
			processedValue, err := bi.processReferenceValue(arrayValue)
			if err != nil {
				return fmt.Errorf("processing array element at index %d: %w", arrayIndex, err)
			}

			// Skip null references
			if processedValue == nil {
				continue
			}

			_, err = junctionStmt.ExecContext(ctx,
				tableData.Language,
				row.Index,
				arrayIndex,
				processedValue)
			if err != nil {
				return fmt.Errorf("inserting junction row for %s[%d]: %w", columnName, arrayIndex, err)
			}
		}
	}

	return nil
}

// convertToSlice converts various array types to []interface{} (internal method)
func (bi *BulkInserter) convertToSlice(value interface{}) ([]interface{}, error) {
	switch v := value.(type) {
	case []interface{}:
		return v, nil
	case []string:
		result := make([]interface{}, len(v))
		for i, val := range v {
			result[i] = val
		}
		return result, nil
	case []bool:
		result := make([]interface{}, len(v))
		for i, val := range v {
			result[i] = val
		}
		return result, nil
	case []int16:
		result := make([]interface{}, len(v))
		for i, val := range v {
			result[i] = val
		}
		return result, nil
	case []uint16:
		result := make([]interface{}, len(v))
		for i, val := range v {
			result[i] = val
		}
		return result, nil
	case []int32:
		result := make([]interface{}, len(v))
		for i, val := range v {
			result[i] = val
		}
		return result, nil
	case []uint32:
		result := make([]interface{}, len(v))
		for i, val := range v {
			result[i] = val
		}
		return result, nil
	case []int64:
		result := make([]interface{}, len(v))
		for i, val := range v {
			result[i] = val
		}
		return result, nil
	case []uint64:
		result := make([]interface{}, len(v))
		for i, val := range v {
			result[i] = val
		}
		return result, nil
	case []float32:
		result := make([]interface{}, len(v))
		for i, val := range v {
			result[i] = val
		}
		return result, nil
	case []float64:
		result := make([]interface{}, len(v))
		for i, val := range v {
			result[i] = val
		}
		return result, nil
	case []*int32:
		result := make([]interface{}, len(v))
		for i, val := range v {
			if val != nil {
				result[i] = *val
			} else {
				result[i] = nil
			}
		}
		return result, nil
	case []*uint32:
		result := make([]interface{}, len(v))
		for i, val := range v {
			if val != nil {
				result[i] = *val
			} else {
				result[i] = nil
			}
		}
		return result, nil
	case []*int64:
		result := make([]interface{}, len(v))
		for i, val := range v {
			if val != nil {
				result[i] = *val
			} else {
				result[i] = nil
			}
		}
		return result, nil
	case []*uint64:
		result := make([]interface{}, len(v))
		for i, val := range v {
			if val != nil {
				result[i] = *val
			} else {
				result[i] = nil
			}
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unsupported array type: %T", value)
	}
}
