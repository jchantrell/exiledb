package dat

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"math"
	"strconv"
)

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

// DATParser parses Path of Exile DAT files
type DATParser struct {
	// Parser width (32 or 64 bit)
	width ParserWidth

	// Options for parsing behavior
	options *ParserOptions
}

// parseState tracks the current row during DAT file parsing
type parseState struct {
	parser     *DATParser
	currentRow int // Current row being parsed (for error messages)
}

// ParserOptions configures DAT parsing behavior
type ParserOptions struct {
	// StrictMode enables strict validation of DAT file format
	StrictMode bool

	// ValidateReferences checks that foreign key references point to valid rows
	ValidateReferences bool

	// MaxStringLength limits the maximum length of strings to prevent memory issues
	MaxStringLength int

	// MaxArrayCount limits the maximum number of array elements
	MaxArrayCount int

	// ArraySizeWarningThreshold sets the threshold for logging warnings about large arrays
	ArraySizeWarningThreshold int
}

const (
	// NullRowSentinel is the sentinel value indicating a null row reference
	NullRowSentinel uint32 = 0xfefe_fefe

	// LongIDNullSentinel is the 64-bit sentinel value for null LongID references
	LongIDNullSentinel uint64 = 0xfefe_fefe_fefe_fefe

	// SentinelValue32 is the 32-bit sentinel value for null strings/arrays
	SentinelValue32 uint32 = 0xFEFEFEFE

	// SentinelValue64 is the 64-bit sentinel value for null strings/arrays
	SentinelValue64 uint64 = 0xFEFEFEFEFEFEFEFE

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

	// UTF-16 surrogate pair constants for Unicode decoding
	UTF16HighSurrogateStart      = 0xD800
	UTF16HighSurrogateEnd        = 0xDBFF
	UTF16LowSurrogateStart       = 0xDC00
	UTF16SupplementaryPlaneStart = 0x10000

	// Default configuration values
	DefaultMaxStringLength           = 65536 // 64KB max string length
	DefaultMaxArrayCount             = 65536 // Match reference implementations
	DefaultArraySizeWarningThreshold = 1000  // Warn when arrays exceed 1000 elements
)

var BoundaryMarker = []byte{0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb}

func NewDATParser() *DATParser {
	return &DATParser{
		options: &ParserOptions{
			StrictMode:                false,
			ValidateReferences:        false,
			MaxStringLength:           DefaultMaxStringLength,
			MaxArrayCount:             DefaultMaxArrayCount,
			ArraySizeWarningThreshold: DefaultArraySizeWarningThreshold,
		},
	}
}

func (p *DATParser) ParseDATFile(ctx context.Context, r io.Reader, schema *TableSchema) (*ParsedTable, error) {
	return p.ParseDATFileWithFilename(ctx, r, "", schema)
}

func (p *DATParser) ParseDATFileWithFilename(ctx context.Context, r io.Reader, filename string, schema *TableSchema) (*ParsedTable, error) {
	if schema == nil {
		return nil, fmt.Errorf("schema cannot be nil")
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("reading DAT file: %w", err)
	}

	if len(data) < MinDATFileSize {
		return nil, fmt.Errorf("DAT file too small: %d bytes (minimum %d)", len(data), MinDATFileSize)
	}

	datFile, err := p.parseDATStructure(data)
	if err != nil {
		return nil, fmt.Errorf("parsing DAT structure: %w", err)
	}

	if filename != "" {
		p.width = WidthForFilename(filename)
	} else {
		p.width = Width64
	}

	rowSize := p.CalculateRowSize(schema, p.width)
	if rowSize == 0 {
		return nil, fmt.Errorf("calculated row size is zero for table %s", schema.Name)
	}

	actualBoundaryPos := p.findAlignedBoundaryMarker(data[4:], datFile.RowCount)
	if actualBoundaryPos == -1 {
		return nil, fmt.Errorf("aligned boundary marker not found in .datc64 file (file size: %d bytes, row count: %d, schema: %s)",
			len(data), datFile.RowCount, schema.Name)
	}
	actualBoundaryPos += 4

	actualFixedDataSize := actualBoundaryPos - 4
	if datFile.RowCount > 0 {
		actualRowSize := actualFixedDataSize / datFile.RowCount
		if actualFixedDataSize%datFile.RowCount != 0 {
			return nil, fmt.Errorf("boundary position %d does not align with row count %d (remainder: %d bytes)",
				actualBoundaryPos, datFile.RowCount, actualFixedDataSize%datFile.RowCount)
		}
		if actualRowSize != rowSize {
			slog.Debug("Row size adjusted", "table", schema.Name, "from", rowSize, "to", actualRowSize)
		}
		rowSize = actualRowSize
	}

	expectedFixedSize := datFile.RowCount * rowSize
	if len(datFile.FixedData) != expectedFixedSize {
		return nil, fmt.Errorf("fixed data size mismatch: expected %d bytes (%d rows * %d bytes/row), got %d bytes",
			expectedFixedSize, datFile.RowCount, rowSize, len(datFile.FixedData))
	}

	state := &parseState{
		parser: p,
	}

	rows := make([]ParsedRow, datFile.RowCount)
	maxFieldsParsed := 0
	successfulRows := 0
	failedRows := 0

	for i := 0; i < datFile.RowCount; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		state.currentRow = i
		rowData := datFile.FixedData[i*rowSize : (i+1)*rowSize]

		row, err := p.parseRow(i, rowData, datFile.DynamicData, schema, state)
		if err != nil {
			failedRows++
			slog.Error("Row parsing failed",
				"filename", filename,
				"table", schema.Name,
				"row_index", i,
				"error", err)
			return nil, fmt.Errorf("parsing row %d: %w", i, err)
		}
		rows[i] = *row
		successfulRows++

		if row.FieldsParsed > maxFieldsParsed {
			maxFieldsParsed = row.FieldsParsed
		}
	}

	return &ParsedTable{
		Schema:   schema,
		RowCount: datFile.RowCount,
		Rows:     rows,
		Metadata: &ParseMetadata{
			FixedDataSize:   len(datFile.FixedData),
			DynamicDataSize: len(datFile.DynamicData),
			TotalFileSize:   len(data),
			MaxFieldsParsed: maxFieldsParsed,
		},
	}, nil
}

func (p *DATParser) parseDATStructure(data []byte) (*DatFile, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("file too small to contain row count")
	}

	rowCountRaw := binary.LittleEndian.Uint32(data[0:4])
	rowCount := int32(rowCountRaw)

	if rowCount > MaxRowCount {
		return nil, fmt.Errorf("row count %d exceeds reasonable limit", rowCount)
	}

	boundaryIndex := p.findAlignedBoundaryMarker(data[4:], int(rowCount))
	if boundaryIndex == -1 {
		return nil, fmt.Errorf("aligned boundary marker not found in DAT file (file size: %d bytes, row count: %d)",
			len(data), rowCount)
	}

	boundaryIndex += 4

	fixedData := data[4:boundaryIndex]

	dynamicDataStart := boundaryIndex
	if dynamicDataStart > len(data) {
		return nil, fmt.Errorf("dynamic data section extends beyond file")
	}
	dynamicData := data[dynamicDataStart:]

	return &DatFile{
		RowCount:    int(rowCount),
		RowLength:   0,
		FixedData:   fixedData,
		DynamicData: dynamicData,
	}, nil
}

func (p *DATParser) CalculateRowSize(schema *TableSchema, width ParserWidth) int {
	totalSize := 0

	maxFields := len(schema.Columns)

	fieldsProcessed := 0
	for _, column := range schema.Columns {
		if fieldsProcessed >= maxFields {
			break
		}

		var fieldSize int

		if column.Array {
			fieldSize = TypeArray.Size(width)
		} else {
			fieldSize = column.Type.Size(width)
		}

		totalSize += fieldSize
		fieldsProcessed++
	}

	return totalSize
}

func (p *DATParser) parseRow(index int, rowData []byte, dynamicData []byte, schema *TableSchema, state *parseState) (*ParsedRow, error) {
	fields := make(map[string]interface{})
	fieldsParsed := 0
	currentOffset := 0

	for i, column := range schema.Columns {
		if i >= len(schema.Columns) {
			break
		}

		name := p.resolveFieldName(&column, i)
		fieldSize := p.calculateFieldSize(&column)

		fieldData, newOffset, shouldBreak := p.extractFieldData(rowData, currentOffset, fieldSize, name)
		if shouldBreak {
			break
		}
		currentOffset = newOffset

		value, err := p.parseFieldValue(fieldData, &column, dynamicData, state)
		if err != nil {
			slog.Debug("Could not read field", "name", name, "fieldStart", currentOffset-fieldSize)
			break
		}

		fields[name] = value
		fieldsParsed++
	}

	return &ParsedRow{
		Index:        index,
		Fields:       fields,
		FieldsParsed: fieldsParsed,
	}, nil
}

func (p *DATParser) resolveFieldName(column *TableColumn, index int) string {
	if column.Name == nil {
		return "Unknown" + strconv.Itoa(index)
	}
	return *column.Name
}

func (p *DATParser) calculateFieldSize(column *TableColumn) int {
	if column.Array {
		return TypeArray.Size(p.width)
	}

	fieldSize := column.Type.Size(p.width)
	if column.Interval {
		fieldSize *= 2
	}
	return fieldSize
}

func (p *DATParser) extractFieldData(rowData []byte, currentOffset, fieldSize int, name string) ([]byte, int, bool) {
	fieldStart := currentOffset
	fieldEnd := currentOffset + fieldSize

	if fieldStart >= len(rowData) {
		slog.Debug("Field exceeds row data length", "name", name, "fieldStart", fieldStart, "rowLength", len(rowData))
		return nil, currentOffset, true
	}

	if fieldEnd > len(rowData) {
		fieldEnd = len(rowData)
	}

	fieldData := rowData[fieldStart:fieldEnd]
	newOffset := currentOffset + fieldSize

	if newOffset > len(rowData) {
		return fieldData, newOffset, true
	}

	return fieldData, newOffset, false
}

func (p *DATParser) parseFieldValue(fieldData []byte, column *TableColumn, dynamicData []byte, state *parseState) (interface{}, error) {
	if column.Array {
		return p.readArrayField(fieldData, column, dynamicData, state)
	}
	return p.readScalarField(fieldData, column, dynamicData, state)
}

func (p *DATParser) readScalarField(data []byte, column *TableColumn, dynamicData []byte, state ...*parseState) (interface{}, error) {
	var statePtr *parseState
	if len(state) > 0 {
		statePtr = state[0]
	}

	requiredBytes := column.Type.Size(p.width)
	if len(data) < requiredBytes {
		return nil, fmt.Errorf("field %s: insufficient data", column.Type)
	}

	switch column.Type {
	case TypeBool:
		return data[0] != 0, nil

	case TypeInt16:
		return int16(binary.LittleEndian.Uint16(data)), nil

	case TypeUint16:
		return binary.LittleEndian.Uint16(data), nil

	case TypeInt32:
		return int32(binary.LittleEndian.Uint32(data)), nil

	case TypeUint32:
		return binary.LittleEndian.Uint32(data), nil

	case TypeInt64:
		return int64(binary.LittleEndian.Uint64(data)), nil

	case TypeUint64:
		return binary.LittleEndian.Uint64(data), nil

	case TypeFloat32:
		return readFloat32At(data, 0), nil

	case TypeFloat64:
		return readFloat64At(data, 0), nil

	case TypeString:
		offset := binary.LittleEndian.Uint32(data)
		return p.ReadString(dynamicData, uint64(offset), statePtr)

	case TypeRow, TypeForeignRow, TypeEnumRow:
		value := binary.LittleEndian.Uint32(data)
		if value == NullRowSentinel {
			return nil, nil
		}
		return &value, nil

	case TypeLongID:
		if p.width == Width32 {
			value := binary.LittleEndian.Uint64(data)
			if value == LongIDNullSentinel {
				return nil, nil
			}
			return &value, nil
		} else {
			value := binary.LittleEndian.Uint64(data[0:8])
			high := binary.LittleEndian.Uint64(data[8:16])

			if value == LongIDNullSentinel && high == LongIDNullSentinel {
				return nil, nil
			}
			if high != 0 {
				return nil, fmt.Errorf("unexpected value in high half of LongID: %016x %016x", value, high)
			}
			return &value, nil
		}

	default:
		return nil, fmt.Errorf("field %s: unsupported type", column.Type)
	}
}

func (p *DATParser) validateArrayFieldInput(data []byte, dynamicData []byte) (count, offset uint64, err error) {
	arraySize := TypeArray.Size(p.width)
	if len(data) < arraySize {
		return 0, 0, fmt.Errorf("array field: insufficient data for %d-bit (need %d bytes)", int(p.width), arraySize)
	}

	count = uint64(binary.LittleEndian.Uint32(data[0:4]))
	if p.width == Width64 {
		offset = uint64(binary.LittleEndian.Uint32(data[8:12]))
	} else {
		offset = uint64(binary.LittleEndian.Uint32(data[4:8]))
	}

	return count, offset, nil
}

func (p *DATParser) readArrayField(data []byte, column *TableColumn, dynamicData []byte, state ...*parseState) (interface{}, error) {
	count, offset, err := p.validateArrayFieldInput(data, dynamicData)
	if err != nil {
		return nil, err
	}

	var statePtr *parseState
	if len(state) > 0 {
		statePtr = state[0]
	}

	if err := p.validateArraySize(count, column, statePtr); err != nil {
		return nil, err
	}

	return p.ReadArray(dynamicData, offset, count, column.Type, statePtr)
}

func (p *DATParser) ReadString(dynamicData []byte, offset uint64, state *parseState) (string, error) {
	if offset == 0 {
		return "", nil
	}

	if offset == uint64(SentinelValue32) || (p.width == Width64 && offset == SentinelValue64) {
		return "", nil
	}

	if offset < MinOffsetForArraysAndStrings {
		return "", nil
	}

	if offset >= uint64(len(dynamicData)) {
		return "", fmt.Errorf("string: offset %d exceeds dynamic data size %d", offset, len(dynamicData))
	}

	data := dynamicData[offset:]
	var result []uint16

	for i := 0; i < len(data)-1; i += 2 {
		ch := binary.LittleEndian.Uint16(data[i:])
		if ch == 0 {
			break
		}
		result = append(result, ch)

		if len(result)*2 > p.options.MaxStringLength {
			return "", fmt.Errorf("string: exceeds maximum length %d", p.options.MaxStringLength)
		}
	}

	runes := make([]rune, 0, len(result))
	for i := 0; i < len(result); i++ {
		if result[i] >= UTF16HighSurrogateStart && result[i] <= UTF16HighSurrogateEnd && i+1 < len(result) {
			high := uint32(result[i] - UTF16HighSurrogateStart)
			low := uint32(result[i+1] - UTF16LowSurrogateStart)
			codepoint := UTF16SupplementaryPlaneStart + (high << 10) + low
			runes = append(runes, rune(codepoint))
			i++
		} else {
			runes = append(runes, rune(result[i]))
		}
	}

	return string(runes), nil
}

func (p *DATParser) ReadArray(dynamicData []byte, offset uint64, count uint64, elementType FieldType, state *parseState) (interface{}, error) {
	if offset == 0 || count == 0 {
		return createEmptyArray(elementType), nil
	}

	if offset == uint64(SentinelValue32) || (p.width == Width64 && offset == SentinelValue64) {
		return createEmptyArray(elementType), nil
	}

	if offset < MinOffsetForArraysAndStrings {
		return nil, fmt.Errorf("array: offset %d too small (minimum %d)", offset, MinOffsetForArraysAndStrings)
	}

	if offset >= uint64(len(dynamicData)) {
		return nil, fmt.Errorf("array: offset %d exceeds dynamic data size %d", offset, len(dynamicData))
	}

	if count == 0 {
		return createEmptyArray(elementType), nil
	}

	data := dynamicData[offset:]
	elementSize := elementType.Size(p.width)

	if state != nil && p.width == Width64 && (elementType == TypeForeignRow || elementType == TypeEnumRow) {
		elementSize = ElementSize64BitForeignRow
	}

	if elementType == TypeString {
		result, err := p.readStringArray(data, dynamicData, count, state)
		if err != nil {
			return nil, err
		}
		return result, nil
	}

	totalSize := int(count) * elementSize
	if totalSize > len(data) {
		return nil, fmt.Errorf("array: data exceeds available data (need %d bytes, have %d)", totalSize, len(data))
	}

	result, err := p.readTypedArray(data[:totalSize], count, elementType)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (p *DATParser) readStringArray(data []byte, dynamicData []byte, count uint64, state *parseState) ([]string, error) {
	uint32Size := TypeUint32.Size(p.width)
	requiredBytes := int(count) * uint32Size
	if requiredBytes > len(data) {
		return nil, fmt.Errorf("string array: offsets exceed available data")
	}

	strings := make([]string, count)
	for i := uint64(0); i < count; i++ {
		offset := uint64(binary.LittleEndian.Uint32(data[i*uint64(uint32Size):]))
		str, err := p.ReadString(dynamicData, offset, state)
		if err != nil {
			return nil, fmt.Errorf("string array: reading string at index %d: %w", i, err)
		}
		strings[i] = str
	}

	return strings, nil
}

// Helper functions for reading binary data using direct binary.LittleEndian calls
func readUint32At(data []byte, offset uint64) uint32 {
	return binary.LittleEndian.Uint32(data[offset:])
}

func readUint64At(data []byte, offset uint64) uint64 {
	return binary.LittleEndian.Uint64(data[offset:])
}

func readFloat32At(data []byte, offset uint64) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(data[offset:]))
}

func readFloat64At(data []byte, offset uint64) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(data[offset:]))
}

func (p *DATParser) readTypedArray(data []byte, count uint64, elementType FieldType) (interface{}, error) {
	switch elementType {
	case TypeBool:
		result := make([]bool, count)
		for i := uint64(0); i < count; i++ {
			result[i] = data[i] != 0
		}
		return result, nil

	case TypeInt16:
		result := make([]int16, count)
		elementSize := uint64(TypeInt16.Size(p.width))
		for i := uint64(0); i < count; i++ {
			offset := i * elementSize
			result[i] = int16(binary.LittleEndian.Uint16(data[offset:]))
		}
		return result, nil

	case TypeUint16:
		result := make([]uint16, count)
		elementSize := uint64(TypeUint16.Size(p.width))
		for i := uint64(0); i < count; i++ {
			offset := i * elementSize
			result[i] = binary.LittleEndian.Uint16(data[offset:])
		}
		return result, nil

	case TypeInt32:
		result := make([]int32, count)
		elementSize := uint64(TypeInt32.Size(p.width))
		for i := uint64(0); i < count; i++ {
			offset := i * elementSize
			result[i] = int32(readUint32At(data, offset))
		}
		return result, nil

	case TypeUint32:
		result := make([]uint32, count)
		elementSize := uint64(TypeUint32.Size(p.width))
		for i := uint64(0); i < count; i++ {
			offset := i * elementSize
			result[i] = readUint32At(data, offset)
		}
		return result, nil

	case TypeInt64:
		result := make([]int64, count)
		elementSize := uint64(TypeInt64.Size(p.width))
		for i := uint64(0); i < count; i++ {
			offset := i * elementSize
			result[i] = int64(readUint64At(data, offset))
		}
		return result, nil

	case TypeUint64:
		result := make([]uint64, count)
		elementSize := uint64(TypeUint64.Size(p.width))
		for i := uint64(0); i < count; i++ {
			offset := i * elementSize
			result[i] = readUint64At(data, offset)
		}
		return result, nil

	case TypeFloat32:
		result := make([]float32, count)
		elementSize := uint64(TypeFloat32.Size(p.width))
		for i := uint64(0); i < count; i++ {
			offset := i * elementSize
			result[i] = readFloat32At(data, offset)
		}
		return result, nil

	case TypeFloat64:
		result := make([]float64, count)
		elementSize := uint64(TypeFloat64.Size(p.width))
		for i := uint64(0); i < count; i++ {
			offset := i * elementSize
			result[i] = readFloat64At(data, offset)
		}
		return result, nil

	case TypeRow, TypeForeignRow, TypeEnumRow:
		result := make([]*uint32, count)
		elementStride := elementType.Size(p.width)

		uint32Size := TypeUint32.Size(p.width)
		for i := uint64(0); i < count; i++ {
			offset := i * uint64(elementStride)
			if int(offset+uint64(uint32Size)) > len(data) {
				return nil, fmt.Errorf("array: element %d exceeds data bounds", i)
			}
			value := binary.LittleEndian.Uint32(data[offset : offset+uint64(uint32Size)])

			if p.isValidForeignKeyValue(value) {
				result[i] = &value
			} else {
				result[i] = nil
			}
		}
		return result, nil

	default:
		return nil, fmt.Errorf("array: unsupported element type %s", elementType)
	}
}

func (p *DATParser) isValidForeignKeyValue(value uint32) bool {
	if value == NullRowSentinel || value == 0 {
		return false
	}

	return value <= MaxReasonableForeignKeyIndex
}

func (p *DATParser) validateArraySize(count uint64, column *TableColumn, state *parseState) error {
	fieldName := "unknown"
	if column.Name != nil {
		fieldName = *column.Name
	}

	if count > uint64(p.options.MaxArrayCount) {
		return fmt.Errorf("field %s: array count %d exceeds maximum %d", fieldName, count, p.options.MaxArrayCount)
	}

	if count > uint64(p.options.ArraySizeWarningThreshold) {
		slog.Debug("Large array detected", "field", fieldName, "count", count)
	}

	return nil
}

func (p *DATParser) findAlignedBoundaryMarker(data []byte, rowCount int) int {
	boundarySequence := BoundaryMarker
	fromIndex := 0

	if rowCount == 0 {
		return bytes.Index(data, boundarySequence)
	}

	for {
		idx := bytes.Index(data[fromIndex:], boundarySequence)
		if idx == -1 {
			return -1
		}

		idx += fromIndex

		if idx%rowCount == 0 {
			return idx
		}

		fromIndex = idx + 1
	}
}

// emptyArrayMap maps field types to their empty slice constructors
var emptyArrayMap = map[FieldType]func() interface{}{
	TypeBool:       func() interface{} { return []bool{} },
	TypeString:     func() interface{} { return []string{} },
	TypeInt16:      func() interface{} { return []int16{} },
	TypeUint16:     func() interface{} { return []uint16{} },
	TypeInt32:      func() interface{} { return []int32{} },
	TypeUint32:     func() interface{} { return []uint32{} },
	TypeInt64:      func() interface{} { return []int64{} },
	TypeUint64:     func() interface{} { return []uint64{} },
	TypeFloat32:    func() interface{} { return []float32{} },
	TypeFloat64:    func() interface{} { return []float64{} },
	TypeRow:        func() interface{} { return []*uint32{} },
	TypeForeignRow: func() interface{} { return []*uint32{} },
	TypeEnumRow:    func() interface{} { return []*uint32{} },
}

// createEmptyArray creates an empty slice of the appropriate type based on the field type
func createEmptyArray(elementType FieldType) interface{} {
	if constructor, ok := emptyArrayMap[elementType]; ok {
		return constructor()
	}
	return []interface{}{}
}
