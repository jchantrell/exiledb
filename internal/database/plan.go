package database

import (
	"fmt"
	"log/slog"

	"github.com/jchantrell/exiledb/internal/dat"
	"github.com/jchantrell/exiledb/internal/poe"
)

const (
	colIndex       = "_index"
	colLanguage    = "_language"
	colParentIndex = "_parent_index"
	colArrayIndex  = "_array_index"
	colValue       = "value"
)

type planColumn struct {
	sqlName   string
	field     string
	sqlType   string
	refTable  string // empty unless the column is a scalar foreign key
	refColumn string
	column    *dat.TableColumn
}

type planJunction struct {
	tableName string
	sqlName   string
	field     string
	refTable  string
	refColumn string
}

type tablePlan struct {
	sqlName    string
	schemaName string
	columns    []planColumn
	junctions  []planJunction
}

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

		if column.Array && column.References != nil {
			if column.Name == nil {
				slog.Warn("skipping unnamed array reference column", "table", schema.Name, "index", i)
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

		if column.Interval && !column.Array {
			if column.References != nil {
				return nil, fmt.Errorf("table %s column %d (%s): interval columns cannot be foreign keys", schema.Name, i, sqlName)
			}
			sqlType, err := mapDATTypeToSQL(column.Type)
			if err != nil {
				return nil, fmt.Errorf("table %s column %d (%s): %w", schema.Name, i, sqlName, err)
			}
			minField, maxField := dat.IntervalFieldNames(column, i)
			for _, f := range []string{minField, maxField} {
				intervalName := poe.ToSnakeCase(f)
				if err := validateIdentifier(intervalName); err != nil {
					return nil, fmt.Errorf("table %s column %d: %w", schema.Name, i, err)
				}
				plan.columns = append(plan.columns, planColumn{
					sqlName: intervalName,
					field:   f,
					sqlType: sqlType,
					column:  column,
				})
			}
			continue
		}

		col := planColumn{
			sqlName: sqlName,
			field:   field,
			column:  column,
		}

		if column.Array {
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

func referenceTarget(ref *dat.ColumnReference) (table, column string, err error) {
	if ref == nil {
		return "", "", fmt.Errorf("nil reference")
	}
	if ref.Table == "" {
		return "", "", fmt.Errorf("empty table")
	}

	table = poe.ToSnakeCase(ref.Table)
	if err := validateIdentifier(table); err != nil {
		return "", "", err
	}
	column = colIndex
	if ref.Column != nil && *ref.Column != "" {
		column = poe.ToSnakeCase(*ref.Column)
	}
	if err := validateIdentifier(column); err != nil {
		return "", "", err
	}
	return table, column, nil
}

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
	case dat.TypeLongID:
		return "INTEGER", nil // 64-bit row reference; SQLite INTEGER holds it
	case dat.TypeArray:
		return "TEXT", nil // Arrays stored as JSON text
	default:
		return "", fmt.Errorf("unsupported field type: %s", fieldType)
	}
}
