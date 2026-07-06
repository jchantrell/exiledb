package database

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

type SchemaProgressCallback func(current int, total int, description string)

func generateTableDDL(plan *TablePlan) string {
	columns := []string{
		colIndex + " INTEGER",
		colLanguage + " TEXT NOT NULL",
	}
	var foreignKeys []string

	for _, col := range plan.columns {
		columns = append(columns, fmt.Sprintf("%s %s", quoteSQLIdentifier(col.sqlName), col.sqlType))

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

func generateJunctionTableDDL(plan *TablePlan, junction *planJunction) string {
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

type DDLRequest struct {
	DDL         string
	TableName   string
	Description string
}

// CreateSchemas creates every table's DDL in one transaction and returns the
// number of tables created (main plus junction). Foreign keys are never
// enforced during load (see DatabaseOptions), so junction tables need not be
// ordered after their parents.
func CreateSchemas(ctx context.Context, db *Database, plans []*TablePlan, progressCallback SchemaProgressCallback) (int, error) {
	if len(plans) == 0 {
		return 0, nil
	}

	requests := generateAllDDL(plans)
	if err := executeDDL(ctx, db, requests, progressCallback); err != nil {
		return 0, fmt.Errorf("executing DDL: %w", err)
	}

	return len(requests), nil
}

func generateAllDDL(plans []*TablePlan) []DDLRequest {
	var requests []DDLRequest
	for _, plan := range plans {
		requests = append(requests, DDLRequest{
			DDL:         generateTableDDL(plan),
			TableName:   plan.sqlName,
			Description: plan.schemaName,
		})
		for i := range plan.junctions {
			junction := &plan.junctions[i]
			requests = append(requests, DDLRequest{
				DDL:         generateJunctionTableDDL(plan, junction),
				TableName:   junction.tableName,
				Description: fmt.Sprintf("%s.%s", plan.schemaName, junction.field),
			})
		}
	}
	return requests
}

func executeDDL(ctx context.Context, db *Database, requests []DDLRequest, progressCallback SchemaProgressCallback) error {
	if len(requests) == 0 {
		return nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning DDL transaction: %w", err)
	}
	defer tx.Rollback() // Safe to call even after commit

	for i, req := range requests {
		if _, err := tx.ExecContext(ctx, req.DDL); err != nil {
			return fmt.Errorf("executing DDL for %s: %w", req.TableName, err)
		}
		if progressCallback != nil {
			progressCallback(i+1, len(requests), req.Description)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing DDL transaction: %w", err)
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
// contain. Every identifier derived from the remote schema — table names,
// column names, and reference targets — is checked against it before
// reaching DDL generation.
var validIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func validateIdentifier(identifier string) error {
	if !validIdentifier.MatchString(identifier) {
		return fmt.Errorf("invalid SQL identifier %q", identifier)
	}
	return nil
}
