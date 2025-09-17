package dat

import (
	"encoding/json"
	"fmt"

	"github.com/jchantrell/exiledb/internal/cache"
	"github.com/jchantrell/exiledb/internal/utils"
	"io"
	"os"
)

// SchemaManager implements SchemaManager using a cached schema file
type SchemaManager struct {
	schema *CommunitySchema
}

// LoadCachedSchema loads the schema from cache or downloads it if not available
func NewSchemaManager() (*SchemaManager, error) {
	cacheManager := cache.CacheManager()
	schemaPath := cacheManager.GetSchemaPath()

	// Ensure cache directory exists
	if err := cacheManager.EnsureDir(cacheManager.GetCacheDir()); err != nil {
		return nil, fmt.Errorf("creating schema cache directory: %w", err)
	}

	// Always download fresh schema to ensure we have the latest version
	// This is important as the community schema is frequently updated with fixes
	if err := utils.DownloadFile(schemaPath, CommunitySchemaURL); err != nil {
		return nil, fmt.Errorf("downloading schema from %s: %w", CommunitySchemaURL, err)
	}

	// Load schema from file
	file, err := os.Open(schemaPath)
	if err != nil {
		return nil, fmt.Errorf("opening cached schema: %w", err)
	}
	defer file.Close()

	schema, err := parseSchemaFromReader(file)
	if err != nil {
		return nil, fmt.Errorf("parsing cached schema: %w", err)
	}

	return &SchemaManager{schema: schema}, nil
}

// LoadSchema returns the cached schema
func (sm *SchemaManager) LoadSchema() (*CommunitySchema, error) {
	return sm.schema, nil
}

// GetTableSchema retrieves a specific table schema by name
// DEPRECATED: Use GetTableSchemaForVersion for version-aware schema selection
func (sm *SchemaManager) GetTableSchema(tableName string) (*TableSchema, bool) {
	return sm.schema.GetTableSchema(tableName)
}

// GetTableSchemaForVersion retrieves a table schema by name filtered by game version compatibility
func (sm *SchemaManager) GetTableSchemaForVersion(tableName string, gameVersion string) (*TableSchema, error) {
	return sm.schema.GetTableSchemaForVersion(tableName, gameVersion)
}


// GetValidTablesForVersion returns all tables valid for the given game version
func (sm *SchemaManager) GetValidTablesForVersion(version string) ([]TableSchema, error) {
	gameVersion, err := utils.ParseGameVersion(version)
	if err != nil {
		return nil, fmt.Errorf("parsing game version %s: %w", version, err)
	}

	return sm.schema.GetValidTables(gameVersion), nil
}

// IsTableValidForVersion checks if a table is valid for the given game version
func (sm *SchemaManager) IsTableValidForVersion(tableName, version string) (bool, error) {
	schema, exists := sm.GetTableSchema(tableName)
	if !exists {
		return false, nil
	}

	gameVersion, err := utils.ParseGameVersion(version)
	if err != nil {
		return false, fmt.Errorf("parsing game version %s: %w", version, err)
	}

	return schema.ValidFor.IsValidForGame(gameVersion), nil
}

// parseSchemaFromReader parses a CommunitySchema from a JSON reader
func parseSchemaFromReader(r io.Reader) (*CommunitySchema, error) {
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
			return fmt.Errorf("referenced table cannot be empty")
		}
	}

	return nil
}

// isValidValidFor checks if a ValidFor value is valid
func isValidValidFor(vf ValidFor) bool {
	return vf == ValidForPoE1 || vf == ValidForPoE2 || vf == ValidForBoth
}
