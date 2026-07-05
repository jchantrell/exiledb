package dat

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/jchantrell/exiledb/internal/poe"
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

// GetTableSchema finds a table schema by name filtered by game version compatibility
func (cs *CommunitySchema) GetTableSchema(tableName string, gameVersion string) (*TableSchema, error) {
	// Parse game version to determine major version
	majorVersion, err := poe.ParseGameVersion(gameVersion)
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
