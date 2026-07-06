package dat

type FieldType string

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

	TypeRow        FieldType = "row"        // row index (references column in same table)
	TypeForeignRow FieldType = "foreignrow" // row index (references column in foreign table)
	TypeEnumRow    FieldType = "enumrow"    // row index (references foreign table with no columns)
	TypeLongID     FieldType = "longid"     // 64-bit row reference (foreign table)

	TypeArray FieldType = "array" // column is an array of unknown type
)

func (ft FieldType) Valid() bool {
	_, ok := fieldTypes[ft]
	return ok
}

func (ft FieldType) Size() int {
	return fieldTypes[ft].size
}

const (
	NullRowSentinel uint32 = 0xfefe_fefe

	LongIDNullSentinel uint64 = 0xfefe_fefe_fefe_fefe

	// MinDATFileSize is the minimum size for a valid DAT file (4 bytes for row count + 8 bytes boundary)
	MinDATFileSize = 12

	MaxRowCount = 10_000_000

	MinOffsetForArraysAndStrings = 8

	ElementSize64BitForeignRow = 16

	DefaultMaxStringLength           = 65536 // 64KB max string length
	DefaultMaxArrayCount             = 65536 // Match reference implementations
	DefaultArraySizeWarningThreshold = 1000  // Warn when arrays exceed 1000 elements
)

// BoundaryMarker separates the fixed-size row section from the dynamic data
// section in a DAT file.
var BoundaryMarker = []byte{0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb}

type ParsedTable struct {
	Schema *TableSchema
	Rows   []ParsedRow
}

type ParsedRow struct {
	Index  int
	Fields map[string]any
}

type DatFile struct {
	// RowCount is the number of rows in the DAT file (first 4 bytes)
	RowCount int

	FixedData []byte

	DynamicData []byte
}
