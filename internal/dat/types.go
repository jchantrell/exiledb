package dat

import (
	"path"
	"reflect"
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
func (ft FieldType) Size() int {
	// Default to 64-bit sizes for backward compatibility
	return ft.SizeForWidth(Width64)
}

// SizeForWidth returns the size in bytes for this field type based on parser width
// Fixed to match actual .datc64 format based on poe-dat-viewer reference implementation
func (ft FieldType) SizeForWidth(width ParserWidth) int {
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
		if width == Width32 {
			return 8 // 32-bit: 8-byte LongID
		}
		return 16 // 64-bit: 16-byte LongID
	case TypeArray:
		return 16 // FIELD_SIZE.ARRAY - always 16 bytes like poe-dat-viewer
	default:
		return 0
	}
}

// GoType returns the corresponding Go reflect.Type for this FieldType
func (ft FieldType) GoType() reflect.Type {
	switch ft {
	case TypeBool:
		return reflect.TypeOf(bool(false))
	case TypeString:
		return reflect.TypeOf(string(""))
	case TypeInt16:
		return reflect.TypeOf(int16(0))
	case TypeUint16:
		return reflect.TypeOf(uint16(0))
	case TypeInt32:
		return reflect.TypeOf(int32(0))
	case TypeUint32:
		return reflect.TypeOf(uint32(0))
	case TypeInt64:
		return reflect.TypeOf(int64(0))
	case TypeUint64:
		return reflect.TypeOf(uint64(0))
	case TypeFloat32:
		return reflect.TypeOf(float32(0))
	case TypeFloat64:
		return reflect.TypeOf(float64(0))
	case TypeRow, TypeForeignRow, TypeEnumRow:
		// Row references use nullable uint32 pointers for null handling (0xfefe_fefe sentinel)
		return reflect.TypeOf((*uint32)(nil))
	case TypeLongID:
		// LongID references use nullable uint64 pointers for null handling
		return reflect.TypeOf((*uint64)(nil))
	case TypeArray:
		// Arrays are handled dynamically based on the underlying element type
		return reflect.TypeOf([]interface{}{})
	default:
		return reflect.TypeOf(interface{}(nil))
	}
}

// IsArray returns true if this field type represents an array
func (ft FieldType) IsArray(array bool) bool {
	return array || ft == TypeArray
}

// IsReference returns true if this field type represents a row reference
func (ft FieldType) IsReference() bool {
	return ft == TypeRow || ft == TypeForeignRow || ft == TypeEnumRow || ft == TypeLongID
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
	// Starts after the boundary marker (\xbb\xbb\xbb\xbb\xbb\xbb\xbb\xbb)
	DynamicData []byte
}

