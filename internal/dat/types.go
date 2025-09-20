package dat

import (
	"path"
	"strings"
)

// ParserWidth represents the bit width of the DAT file parser
type ParserWidth int

const (
	// Width32 represents 32-bit DAT files (.dat, .datl)
	Width32 ParserWidth = 32
	// Width64 represents 64-bit DAT files (.dat64, .datl64)
	Width64 ParserWidth = 64
)

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

// WidthForFilename determines the parser width based on the DAT file extension
func WidthForFilename(filename string) ParserWidth {
	ext := strings.ToLower(path.Ext(filename))
	switch ext {
	case ".dat", ".datl":
		return Width32 // 32-bit DAT files with UTF-16 (.dat) or UTF-32 (.datl)
	case ".dat64", ".datl64", ".datc64", ".datc":
		return Width64 // 64-bit DAT files
	default:
		// Default to 32-bit for unknown extensions to match original PoE format
		return Width32
	}
}

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

// Size returns the fixed size in bytes for this field type in DAT files
// Returns 0 for variable-length types (strings, arrays)
// If width is provided, it uses that width, otherwise defaults to 64-bit for backward compatibility
func (ft FieldType) Size(width ...ParserWidth) int {
	var w ParserWidth = Width64 // Default to 64-bit for backward compatibility
	if len(width) > 0 {
		w = width[0]
	}

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
		if w == Width32 {
			return 8 // 32-bit: 8-byte LongID
		}
		return 16 // 64-bit: 16-byte LongID
	case TypeArray:
		return 16 // FIELD_SIZE.ARRAY - always 16 bytes like poe-dat-viewer
	default:
		return 0
	}
}

// DatFile represents the structure of a parsed DAT file
type DatFile struct {
	// RowCount is the number of rows in the DAT file (first 4 bytes)
	RowCount int

	// RowLength is the fixed size of each row in bytes
	RowLength int

	// FixedData contains the fixed-size row data section
	FixedData []byte

	// DynamicData contains the variable-length data section (strings, arrays)
	// Starts after the boundary marker (see BoundaryMarker in parser.go)
	DynamicData []byte
}
