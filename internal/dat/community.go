package dat

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/jchantrell/exiledb/internal/utils"
)

// CommunitySchemaURL is the official URL for the community schema
const CommunitySchemaURL = "https://github.com/poe-tool-dev/dat-schema/releases/download/latest/schema.min.json"

// TableColumn represents a column definition from the community schema
type TableColumn struct {
	Name        *string          `json:"name"`
	Description *string          `json:"description"`
	Array       bool             `json:"array"`
	Type        FieldType        `json:"type"`
	Unique      bool             `json:"unique"`
	Localized   bool             `json:"localized"`
	Until       *string          `json:"until"`      // Version when this column was removed
	References  *ColumnReference `json:"references"` // Foreign key reference
	File        *string          `json:"file"`       // File extension for asset files
	Files       []string         `json:"files"`      // Multiple file extensions
	Interval    bool             `json:"interval"`   // Whether this is an interval field
}

// ColumnReference represents a foreign key relationship from the community schema
type ColumnReference struct {
	Table  string  `json:"table"`            // Referenced table name
	Column *string `json:"column,omitempty"` // Referenced column name (optional)
}

// ValidFor represents game version compatibility flags
type ValidFor int

const (
	ValidForPoE1 ValidFor = 0x01 // Path of Exile 1 only
	ValidForPoE2 ValidFor = 0x02 // Path of Exile 2 only
	ValidForBoth ValidFor = 0x03 // Both games (PoE1 | PoE2)
)

// TableSchema represents a complete table definition from the community schema
type TableSchema struct {
	ValidFor ValidFor      `json:"validFor"` // Game version compatibility
	Name     string        `json:"name"`     // Table name (matches DAT filename)
	Columns  []TableColumn `json:"columns"`  // Column definitions
	Tags     []string      `json:"tags"`     // Metadata tags
}

// SchemaEnumeration represents an enumeration definition from the community schema
type SchemaEnumeration struct {
	ValidFor    ValidFor  `json:"validFor"`    // Game version compatibility
	Name        string    `json:"name"`        // Enumeration name
	Indexing    int       `json:"indexing"`    // 0 or 1 based indexing
	Enumerators []*string `json:"enumerators"` // Enumeration values (nullable)
}

// SchemaMetadata contains metadata about the community schema
type SchemaMetadata struct {
	Version   int `json:"version"`   // Schema version number
	CreatedAt int `json:"createdAt"` // Unix timestamp when schema was created
}

// CommunitySchema represents the complete community schema file structure
type CommunitySchema struct {
	SchemaMetadata
	Tables       []TableSchema       `json:"tables"`       // Table definitions
	Enumerations []SchemaEnumeration `json:"enumerations"` // Enumeration definitions
}

// GetSchemaTableNames returns all table names from the schema
func (schema *CommunitySchema) GetSchemaTableNames() []string {
	names := make([]string, len(schema.Tables))
	for i, table := range schema.Tables {
		names[i] = table.Name
	}
	return names
}

// GetTableSchema finds a table schema by name filtered by game version compatibility
func (cs *CommunitySchema) GetTableSchema(tableName string, gameVersion string) (*TableSchema, error) {
	// Parse game version to determine major version
	majorVersion, err := utils.ParseGameVersion(gameVersion)
	if err != nil {
		return nil, fmt.Errorf("parsing game version %s: %w", gameVersion, err)
	}

	// Collect all matching schemas
	var matchingSchemas []*TableSchema

	// Try exact match first for performance
	for i := range cs.Tables {
		if cs.Tables[i].Name == tableName {
			validForGame := cs.Tables[i].ValidFor.IsValidForGame(majorVersion)
			if validForGame {
				matchingSchemas = append(matchingSchemas, &cs.Tables[i])
			}
		}
	}

	// Fall back to case-insensitive match for DAT file names if no exact matches found
	if len(matchingSchemas) == 0 {
		lowerTableName := strings.ToLower(tableName)
		for i := range cs.Tables {
			if strings.ToLower(cs.Tables[i].Name) == lowerTableName {
				validForGame := cs.Tables[i].ValidFor.IsValidForGame(majorVersion)
				if validForGame {
					matchingSchemas = append(matchingSchemas, &cs.Tables[i])
				}
			}
		}
	}

	if len(matchingSchemas) == 0 {
		return nil, fmt.Errorf("no schema found for table %s compatible with game version %s (major: %d)", tableName, gameVersion, majorVersion)
	}

	if len(matchingSchemas) > 1 {
		slog.Warn("Multiple compatible schemas found, using first match",
			"table", tableName,
			"game_version", gameVersion,
			"matching_schemas", len(matchingSchemas))
	}

	selectedSchema := matchingSchemas[0]

	return selectedSchema, nil
}

// GetValidTables returns all tables that are valid for the given game version
func (cs *CommunitySchema) GetValidTables(gameVersion int) []TableSchema {
	var validTables []TableSchema
	for _, table := range cs.Tables {
		if table.ValidFor.IsValidForGame(gameVersion) {
			validTables = append(validTables, table)
		}
	}
	return validTables
}

// IsValidForGame checks if a ValidFor flag is compatible with the given game version
func (vf ValidFor) IsValidForGame(gameVersion int) bool {
	if gameVersion >= 4 {
		// Path of Exile 2 (4.x versions)
		return (vf & ValidForPoE2) != 0
	} else {
		// Path of Exile 1 (3.x versions)
		return (vf & ValidForPoE1) != 0
	}
}

// FilterTablesForVersion filters a list of table names to only include those valid for the given version
func (schema *CommunitySchema) FilterTablesForVersion(tableNames []string, version string) ([]string, error) {

	var validTables []string
	for _, tableName := range tableNames {
		// Use the version-aware schema selection to check compatibility
		table, err := schema.GetTableSchema(tableName, version)
		if err != nil {
			continue // Skip tables that don't have compatible schemas
		}

		// If we got a table schema, it's already validated for this version
		if table != nil {
			validTables = append(validTables, tableName)
		}
	}

	return validTables, nil
}

// GetColumnNames returns all column names for a given table
func (table *TableSchema) GetColumnNames() []string {
	var names []string
	for _, column := range table.Columns {
		if column.Name != nil {
			names = append(names, *column.Name)
		}
	}
	return names
}

// GetColumnByName finds a column by name in a table schema
func (table *TableSchema) GetColumnByName(columnName string) (*TableColumn, bool) {
	for i, column := range table.Columns {
		if column.Name != nil && *column.Name == columnName {
			return &table.Columns[i], true
		}
	}
	return nil, false
}

// HasColumn checks if a table has a column with the given name
func (table *TableSchema) HasColumn(columnName string) bool {
	_, exists := table.GetColumnByName(columnName)
	return exists
}

// GetForeignKeyColumns returns all columns in a table that are foreign key references
func (table *TableSchema) GetForeignKeyColumns() []TableColumn {
	var fkColumns []TableColumn
	for _, column := range table.Columns {
		if column.References != nil {
			fkColumns = append(fkColumns, column)
		}
	}
	return fkColumns
}

// GetArrayColumns returns all columns in a table that are arrays
func (table *TableSchema) GetArrayColumns() []TableColumn {
	var arrayColumns []TableColumn
	for _, column := range table.Columns {
		if column.Array {
			arrayColumns = append(arrayColumns, column)
		}
	}
	return arrayColumns
}

// GetLocalizedColumns returns all columns in a table that are localized
func (table *TableSchema) GetLocalizedColumns() []TableColumn {
	var localizedColumns []TableColumn
	for _, column := range table.Columns {
		if column.Localized {
			localizedColumns = append(localizedColumns, column)
		}
	}
	return localizedColumns
}
