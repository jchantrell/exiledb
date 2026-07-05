package database

import (
	"fmt"

	"github.com/jchantrell/exiledb/internal/dat"
	"github.com/jchantrell/exiledb/internal/poe"
)

// Standard columns present on every generated table, plus the junction table
// columns used for foreign key array storage.
const (
	colIndex       = "_index"
	colLanguage    = "_language"
	colParentIndex = "_parent_index"
	colArrayIndex  = "_array_index"
	colValue       = "value"
)

// planColumn is one SQL column of the main table: its SQL name, the parsed
// row field it is populated from, its SQLite type, and (for scalar foreign
// keys) the referenced table and column.
type planColumn struct {
	sqlName   string
	field     string
	sqlType   string
	refTable  string // empty unless the column is a scalar foreign key
	refColumn string
	column    *dat.TableColumn
}

// planJunction is one named foreign key array column, stored as a junction
// table instead of a main table column.
type planJunction struct {
	tableName string
	sqlName   string
	field     string
	refTable  string
	refColumn string
}

// tablePlan is the canonical relational mapping of one dat.TableSchema: the
// main table name, its ordered columns, and its junction tables. Both DDL
// generation and the insert plan derive from it so CREATE and INSERT can
// never disagree on names, order, or column sets.
type tablePlan struct {
	sqlName    string
	schemaName string
	columns    []planColumn
	junctions  []planJunction
}

// newTablePlan computes the relational layout for a table schema, validating
// every identifier the remote schema contributes to generated SQL.
func newTablePlan(schema *dat.TableSchema) (*tablePlan, error) {
	if schema == nil {
		return nil, fmt.Errorf("table schema cannot be nil")
	}
	if schema.Name == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}

	tableName := poe.ToSnakeCase(schema.Name)
	if err := validateIdentifier(tableName); err != nil {
		return nil, fmt.Errorf("table %s: %w", schema.Name, err)
	}

	plan := &tablePlan{sqlName: tableName, schemaName: schema.Name}

	for i := range schema.Columns {
		column := &schema.Columns[i]
		field := dat.FieldName(column, i)
		sqlName := poe.ToSnakeCase(field)
		if err := validateIdentifier(sqlName); err != nil {
			return nil, fmt.Errorf("table %s column %d: %w", schema.Name, i, err)
		}

		// Foreign key arrays are stored in junction tables, not main table
		// columns. Unnamed ones have no junction table either and are
		// dropped entirely.
		if column.Array && column.References != nil {
			if column.Name == nil {
				continue
			}

			refTable, refColumn, err := referenceTarget(column.References)
			if err != nil {
				return nil, fmt.Errorf("table %s column %d (%s): %w", schema.Name, i, sqlName, err)
			}

			junctionName := fmt.Sprintf("%s_%s_junction", tableName, sqlName)
			if err := validateIdentifier(junctionName); err != nil {
				return nil, fmt.Errorf("table %s column %d: %w", schema.Name, i, err)
			}

			plan.junctions = append(plan.junctions, planJunction{
				tableName: junctionName,
				sqlName:   sqlName,
				field:     field,
				refTable:  refTable,
				refColumn: refColumn,
			})
			continue
		}

		col := planColumn{
			sqlName: sqlName,
			field:   field,
			column:  column,
		}

		if column.Array {
			// Simple arrays are stored as JSON text.
			col.sqlType = "TEXT"
		} else {
			sqlType, err := mapDATTypeToSQL(column.Type)
			if err != nil {
				return nil, fmt.Errorf("table %s column %d (%s): %w", schema.Name, i, sqlName, err)
			}
			col.sqlType = sqlType

			if column.References != nil {
				col.refTable, col.refColumn, err = referenceTarget(column.References)
				if err != nil {
					return nil, fmt.Errorf("table %s column %d (%s): %w", schema.Name, i, sqlName, err)
				}
			}
		}

		plan.columns = append(plan.columns, col)
	}

	return plan, nil
}

// referenceTarget resolves a schema foreign key reference to its SQL table
// and column names, defaulting to the _index column.
func referenceTarget(ref *dat.ColumnReference) (table, column string, err error) {
	if ref == nil {
		return "", "", fmt.Errorf("nil reference")
	}
	if ref.Table == "" {
		return "", "", fmt.Errorf("empty table")
	}

	table = poe.ToSnakeCase(ref.Table)
	column = colIndex
	if ref.Column != nil && *ref.Column != "" {
		column = poe.ToSnakeCase(*ref.Column)
	}
	return table, column, nil
}

// mapDATTypeToSQL maps DAT field types to SQLite types
func mapDATTypeToSQL(fieldType dat.FieldType) (string, error) {
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
