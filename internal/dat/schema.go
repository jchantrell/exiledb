package dat

import (
	"encoding/json"
	"fmt"
	"io"
)

func ParseCommunitySchema(r io.Reader) (*CommunitySchema, error) {
	decoder := json.NewDecoder(r)

	var schema CommunitySchema
	if err := decoder.Decode(&schema); err != nil {
		return nil, fmt.Errorf("decoding JSON schema: %w", err)
	}

	if err := validateSchema(&schema); err != nil {
		return nil, fmt.Errorf("validating schema: %w", err)
	}

	return &schema, nil
}

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

	for i, table := range schema.Tables {
		if err := validateTable(&table); err != nil {
			return fmt.Errorf("invalid table %d (%s): %w", i, table.Name, err)
		}
	}

	return nil
}

func validateTable(table *TableSchema) error {
	if table.Name == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	if !isValidValidFor(table.ValidFor) {
		return fmt.Errorf("invalid validFor value: %d", table.ValidFor)
	}

	for i, column := range table.Columns {
		if err := validateColumn(&column); err != nil {
			return fmt.Errorf("invalid column %d: %w", i, err)
		}
	}

	return nil
}

func validateColumn(column *TableColumn) error {
	if !column.Type.Valid() {
		return fmt.Errorf("invalid field type: %s", column.Type)
	}

	if column.References != nil {
		if column.References.Table == "" {
			return fmt.Errorf("empty table")
		}
	}

	return nil
}

func isValidValidFor(vf ValidFor) bool {
	return vf == ValidForPoE1 || vf == ValidForPoE2 || vf == ValidForBoth
}
