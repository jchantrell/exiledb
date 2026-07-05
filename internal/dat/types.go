package dat

// FieldType represents a DAT file field type from the community schema
type FieldType string

// FieldType constants matching the community schema specification
const (
	TypeBool    FieldType = "bool"
	TypeString  FieldType = "string"
	TypeInt16   FieldType = "i16"
	TypeUint16  FieldType = "u16"
	TypeInt32   FieldType = "i32"
	TypeUint32  FieldType = "u32"
	TypeInt64   FieldType = "i64"
	TypeUint64  FieldType = "u64"
	TypeFloat32 FieldType = "f32"
	TypeFloat64 FieldType = "f64"

	// Reference types
	TypeRow        FieldType = "row"        // row index (references column in same table)
	TypeForeignRow FieldType = "foreignrow" // row index (references column in foreign table)
	TypeEnumRow    FieldType = "enumrow"    // row index (references foreign table with no columns)
	TypeLongID     FieldType = "longid"     // 64-bit row reference (foreign table)

	// Array type (used when array: true and type is one of the above)
	TypeArray FieldType = "array" // column is an array of unknown type
)

// Valid checks if the FieldType is a valid type from the community schema
func (ft FieldType) Valid() bool {
	switch ft {
	case TypeBool, TypeString,
		TypeInt16, TypeUint16, TypeInt32, TypeUint32, TypeInt64, TypeUint64,
		TypeFloat32, TypeFloat64,
		TypeRow, TypeForeignRow, TypeEnumRow, TypeLongID, TypeArray:
		return true
	default:
		return false
	}
}

// Size returns the fixed size in bytes for this field type in 64-bit DAT
// files (.datc64), the only format current game clients ship.
func (ft FieldType) Size() int {
	switch ft {
	case TypeBool:
		return 1
	case TypeInt16, TypeUint16:
		return 2
	case TypeInt32, TypeUint32, TypeFloat32:
		return 4
	case TypeInt64, TypeUint64, TypeFloat64:
		return 8
	case TypeRow:
		return 8 // FIELD_SIZE.KEY - always 8 bytes like poe-dat-viewer
	case TypeForeignRow:
		return 16 // FIELD_SIZE.KEY_FOREIGN - always 16 bytes like poe-dat-viewer
	case TypeEnumRow:
		return 4 // enumrow is always 4 bytes per poe-dat-viewer
	case TypeString:
		return 8 // FIELD_SIZE.STRING - always 8 bytes like poe-dat-viewer
	case TypeLongID:
		return 16 // 64-bit: 16-byte LongID
	case TypeArray:
		return 16 // FIELD_SIZE.ARRAY - always 16 bytes like poe-dat-viewer
	default:
		return 0
	}
}

const (
	// NullRowSentinel is the sentinel value indicating a null row reference
	NullRowSentinel uint32 = 0xfefe_fefe

	// LongIDNullSentinel is the 64-bit sentinel value for null LongID references
	LongIDNullSentinel uint64 = 0xfefe_fefe_fefe_fefe

	// MaxReasonableForeignKeyIndex is the maximum reasonable value for foreign key indices
	MaxReasonableForeignKeyIndex uint32 = 100_000_000

	// MinDATFileSize is the minimum size for a valid DAT file (4 bytes for row count + 8 bytes boundary)
	MinDATFileSize = 12

	// MaxRowCount is the maximum reasonable number of rows in a DAT file
	MaxRowCount = 10_000_000

	// MinOffsetForArraysAndStrings is the minimum offset value for arrays and strings in dynamic data
	MinOffsetForArraysAndStrings = 8

	// ElementSize64BitForeignRow is the element size for foreign/enum row arrays in 64-bit width
	ElementSize64BitForeignRow = 16

	// Default configuration values
	DefaultMaxStringLength           = 65536 // 64KB max string length
	DefaultMaxArrayCount             = 65536 // Match reference implementations
	DefaultArraySizeWarningThreshold = 1000  // Warn when arrays exceed 1000 elements
)

// BoundaryMarker separates the fixed-size row section from the dynamic data
// section in a DAT file.
var BoundaryMarker = []byte{0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb}

// ParsedTable represents a completely parsed DAT table with all rows and metadata
type ParsedTable struct {
	Schema   *TableSchema   `json:"schema"`   // Original table schema
	RowCount int            `json:"rowCount"` // Number of rows parsed
	Rows     []ParsedRow    `json:"rows"`     // All parsed row data
	Metadata *ParseMetadata `json:"metadata"` // Parsing metadata
}

// ParsedRow represents a single parsed row from a DAT table
type ParsedRow struct {
	Index        int                    `json:"index"`        // Row index (0-based)
	Fields       map[string]interface{} `json:"fields"`       // Field name to parsed value mapping
	FieldsParsed int                    `json:"fieldsParsed"` // Number of fields successfully parsed (useful for partial schema)
}

// ParseMetadata contains metadata about the parsing operation
type ParseMetadata struct {
	FixedDataSize   int `json:"fixedDataSize"`   // Size of fixed data section
	DynamicDataSize int `json:"dynamicDataSize"` // Size of dynamic data section
	TotalFileSize   int `json:"totalFileSize"`   // Total DAT file size
	MaxFieldsParsed int `json:"maxFieldsParsed"` // Maximum number of fields parsed in any row (for partial schema)
}

// DatFile represents the structure of a parsed DAT file
type DatFile struct {
	// RowCount is the number of rows in the DAT file (first 4 bytes)
	RowCount int

	// FixedData contains the fixed-size row data section
	FixedData []byte

	// DynamicData contains the variable-length data section (strings, arrays)
	// Starts after the boundary marker (see BoundaryMarker in parser.go)
	DynamicData []byte
}
