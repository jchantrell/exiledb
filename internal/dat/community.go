package dat

const CommunitySchemaURL = "https://github.com/poe-tool-dev/dat-schema/releases/download/latest/schema.min.json"

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

type ColumnReference struct {
	Table  string  `json:"table"`            // Referenced table name
	Column *string `json:"column,omitempty"` // Referenced column name (optional)
}

type ValidFor int

const (
	ValidForPoE1 ValidFor = 0x01 // Path of Exile 1 only
	ValidForPoE2 ValidFor = 0x02 // Path of Exile 2 only
	ValidForBoth ValidFor = 0x03 // Both games (PoE1 | PoE2)
)

type TableSchema struct {
	ValidFor ValidFor      `json:"validFor"` // Game version compatibility
	Name     string        `json:"name"`     // Table name (matches DAT filename)
	Columns  []TableColumn `json:"columns"`  // Column definitions
	Tags     []string      `json:"tags"`     // Metadata tags
}

type SchemaMetadata struct {
	Version   int `json:"version"`   // Schema version number
	CreatedAt int `json:"createdAt"` // Unix timestamp when schema was created
}

type CommunitySchema struct {
	SchemaMetadata
	Tables []TableSchema `json:"tables"` // Table definitions
}

func (cs *CommunitySchema) GetValidTables(gameVersion int) []TableSchema {
	var validTables []TableSchema
	for _, table := range cs.Tables {
		if table.ValidFor.IsValidForGame(gameVersion) {
			validTables = append(validTables, table)
		}
	}
	return validTables
}

func (vf ValidFor) IsValidForGame(gameVersion int) bool {
	if gameVersion >= 4 {
		return (vf & ValidForPoE2) != 0
	} else {
		return (vf & ValidForPoE1) != 0
	}
}
