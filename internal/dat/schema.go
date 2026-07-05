package dat

import (
	"encoding/json"
	"fmt"
	"io"
)

// ParseCommunitySchema parses and validates a community schema from JSON.
// Fetching the schema is the caller's concern; this package only decodes.
func ParseCommunitySchema(r io.Reader) (*CommunitySchema, error) {
	decoder := json.NewDecoder(r)

	var schema CommunitySchema
	if err := decoder.Decode(&schema); err != nil {
		return nil, fmt.Errorf("decoding JSON schema: %w", err)
	}

	// Validate the parsed schema
	if err := validateSchema(&schema); err != nil {
		return nil, fmt.Errorf("validating schema: %w", err)
	}

	return &schema, nil
}

// validateSchema performs basic validation on a loaded schema
func validateSchema(schema *CommunitySchema) error {
	if schema.Version <= 0 {
		return fmt.Errorf("invalid schema version: %d", schema.Version)
	}

	if schema.CreatedAt <= 0 {
		return fmt.Errorf("invalid schema creation timestamp: %d", schema.CreatedAt)
	}

	if len(schema.Tables) == 0 {
		return fmt.Errorf("schema contains no tables")
	}

	// Validate each table
	for i, table := range schema.Tables {
		if err := validateTable(&table); err != nil {
			return fmt.Errorf("invalid table %d (%s): %w", i, table.Name, err)
		}
	}

	return nil
}

// validateTable validates a single table schema
func validateTable(table *TableSchema) error {
	if table.Name == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	if !isValidValidFor(table.ValidFor) {
		return fmt.Errorf("invalid validFor value: %d", table.ValidFor)
	}

	// Validate columns
	for i, column := range table.Columns {
		if err := validateColumn(&column); err != nil {
			return fmt.Errorf("invalid column %d: %w", i, err)
		}
	}

	return nil
}

// validateColumn validates a single column definition
func validateColumn(column *TableColumn) error {
	if !column.Type.Valid() {
		return fmt.Errorf("invalid field type: %s", column.Type)
	}

	// If column has references, validate the reference
	if column.References != nil {
		if column.References.Table == "" {
			return fmt.Errorf("empty table")
		}
	}

	return nil
}

// isValidValidFor checks if a ValidFor value is valid
func isValidValidFor(vf ValidFor) bool {
	return vf == ValidForPoE1 || vf == ValidForPoE2 || vf == ValidForBoth
}
