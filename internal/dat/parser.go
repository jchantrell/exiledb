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
	"unicode/utf16"
)

// DATParser parses Path of Exile DAT files
type DATParser struct {
	options *ParserOptions
}

// ParserOptions configures DAT parsing behavior
type ParserOptions struct {
	MaxStringLength           int // Maximum string length, to prevent memory issues
	MaxArrayCount             int // Maximum number of array elements
	ArraySizeWarningThreshold int // Threshold for logging warnings about large arrays
}

func NewDATParser() *DATParser {
	return &DATParser{
		options: &ParserOptions{
			MaxStringLength:           DefaultMaxStringLength,
			MaxArrayCount:             DefaultMaxArrayCount,
			ArraySizeWarningThreshold: DefaultArraySizeWarningThreshold,
		},
	}
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

	datFile, err := parseDATStructure(data)
	if err != nil {
		return nil, fmt.Errorf("parsing DAT structure: %w", err)
	}

	width := Width64
	if filename != "" {
		width = WidthForFilename(filename)
	}

	rowSize := calculateRowSize(schema, width)
	if rowSize == 0 {
		return nil, fmt.Errorf("calculated row size is zero for table %s", schema.Name)
	}

	// The boundary marker position is authoritative for the actual row size;
	// the schema-derived size is only a starting estimate.
	if datFile.RowCount > 0 {
		fixedSize := len(datFile.FixedData)
		if fixedSize%datFile.RowCount != 0 {
			return nil, fmt.Errorf("boundary position %d does not align with row count %d (remainder: %d bytes)",
				fixedSize+4, datFile.RowCount, fixedSize%datFile.RowCount)
		}
		actualRowSize := fixedSize / datFile.RowCount
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

	d := &decoder{
		width:   width,
		dynamic: datFile.DynamicData,
		options: p.options,
	}

	rows := make([]ParsedRow, datFile.RowCount)
	maxFieldsParsed := 0

	for i := 0; i < datFile.RowCount; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		rowData := datFile.FixedData[i*rowSize : (i+1)*rowSize]
		rows[i] = d.parseRow(i, rowData, schema)

		if rows[i].FieldsParsed > maxFieldsParsed {
			maxFieldsParsed = rows[i].FieldsParsed
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

func parseDATStructure(data []byte) (*DatFile, error) {
	rowCountRaw := binary.LittleEndian.Uint32(data[0:4])
	if rowCountRaw > MaxRowCount {
		return nil, fmt.Errorf("row count %d exceeds reasonable limit", rowCountRaw)
	}
	rowCount := int(rowCountRaw)

	boundaryIndex := findAlignedBoundaryMarker(data[4:], rowCount)
	if boundaryIndex == -1 {
		return nil, fmt.Errorf("aligned boundary marker not found in DAT file (file size: %d bytes, row count: %d)",
			len(data), rowCount)
	}
	boundaryIndex += 4

	return &DatFile{
		RowCount:    rowCount,
		FixedData:   data[4:boundaryIndex],
		DynamicData: data[boundaryIndex:],
	}, nil
}

// fieldSize is the single owner of per-column fixed-data width; row size and
// field offsets must agree byte-for-byte, so both derive from it.
func fieldSize(column *TableColumn, width ParserWidth) int {
	if column.Array {
		return TypeArray.Size(width)
	}
	size := column.Type.Size(width)
	if column.Interval {
		size *= 2
	}
	return size
}

func calculateRowSize(schema *TableSchema, width ParserWidth) int {
	totalSize := 0
	for i := range schema.Columns {
		totalSize += fieldSize(&schema.Columns[i], width)
	}
	return totalSize
}

func findAlignedBoundaryMarker(data []byte, rowCount int) int {
	if rowCount == 0 {
		return bytes.Index(data, BoundaryMarker)
	}

	fromIndex := 0
	for {
		idx := bytes.Index(data[fromIndex:], BoundaryMarker)
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

// decoder holds the per-file decode context. It is created per parse call so
// a shared DATParser carries no per-file state.
type decoder struct {
	width   ParserWidth
	dynamic []byte
	options *ParserOptions
}

func (d *decoder) parseRow(index int, rowData []byte, schema *TableSchema) ParsedRow {
	fields := make(map[string]interface{})
	fieldsParsed := 0
	offset := 0

	for i, column := range schema.Columns {
		name := fieldName(&column, i)
		size := fieldSize(&column, d.width)

		if offset >= len(rowData) {
			slog.Debug("Field exceeds row data length", "name", name, "fieldStart", offset, "rowLength", len(rowData))
			break
		}
		if offset+size > len(rowData) {
			break
		}

		fieldData := rowData[offset : offset+size]
		offset += size

		value, err := d.fieldValue(fieldData, &column)
		if err != nil {
			slog.Debug("Could not read field", "name", name, "fieldStart", offset-size)
			break
		}

		fields[name] = value
		fieldsParsed++
	}

	return ParsedRow{
		Index:        index,
		Fields:       fields,
		FieldsParsed: fieldsParsed,
	}
}

func fieldName(column *TableColumn, index int) string {
	if column.Name == nil {
		return "Unknown" + strconv.Itoa(index)
	}
	return *column.Name
}

func (d *decoder) fieldValue(data []byte, column *TableColumn) (interface{}, error) {
	if column.Array {
		return d.readArrayField(data, column)
	}
	return d.readScalarField(data, column)
}

func (d *decoder) readScalarField(data []byte, column *TableColumn) (interface{}, error) {
	if len(data) < column.Type.Size(d.width) {
		return nil, fmt.Errorf("field %s: insufficient data", column.Type)
	}

	c := codecs[column.Type]
	if c.scalar == nil {
		return nil, fmt.Errorf("field %s: unsupported type", column.Type)
	}
	return c.scalar(d, data)
}

func (d *decoder) readArrayField(data []byte, column *TableColumn) (interface{}, error) {
	headerSize := TypeArray.Size(d.width)
	if len(data) < headerSize {
		return nil, fmt.Errorf("array field: insufficient data for %d-bit (need %d bytes)", int(d.width), headerSize)
	}

	count := uint64(binary.LittleEndian.Uint32(data[0:4]))
	var offset uint64
	if d.width == Width64 {
		offset = uint64(binary.LittleEndian.Uint32(data[8:12]))
	} else {
		offset = uint64(binary.LittleEndian.Uint32(data[4:8]))
	}

	name := "unknown"
	if column.Name != nil {
		name = *column.Name
	}
	if count > uint64(d.options.MaxArrayCount) {
		return nil, fmt.Errorf("field %s: array count %d exceeds maximum %d", name, count, d.options.MaxArrayCount)
	}
	if count > uint64(d.options.ArraySizeWarningThreshold) {
		slog.Debug("Large array detected", "field", name, "count", count)
	}

	return d.readArray(offset, count, column.Type)
}

func (d *decoder) readArray(offset, count uint64, elementType FieldType) (interface{}, error) {
	c := codecs[elementType]

	if offset == 0 || count == 0 ||
		offset == uint64(NullRowSentinel) || (d.width == Width64 && offset == LongIDNullSentinel) {
		if c.slice == nil {
			return []interface{}{}, nil
		}
		return c.slice(d, nil, 0)
	}

	if offset < MinOffsetForArraysAndStrings {
		return nil, fmt.Errorf("array: offset %d too small (minimum %d)", offset, MinOffsetForArraysAndStrings)
	}
	if offset >= uint64(len(d.dynamic)) {
		return nil, fmt.Errorf("array: offset %d exceeds dynamic data size %d", offset, len(d.dynamic))
	}
	if c.slice == nil {
		return nil, fmt.Errorf("array: unsupported element type %s", elementType)
	}

	return c.slice(d, d.dynamic[offset:], count)
}

func (d *decoder) readString(offset uint64) (string, error) {
	if offset == 0 || offset == uint64(NullRowSentinel) || (d.width == Width64 && offset == LongIDNullSentinel) {
		return "", nil
	}

	if offset < MinOffsetForArraysAndStrings {
		return "", nil
	}

	if offset >= uint64(len(d.dynamic)) {
		return "", fmt.Errorf("string: offset %d exceeds dynamic data size %d", offset, len(d.dynamic))
	}

	data := d.dynamic[offset:]
	var result []uint16

	for i := 0; i < len(data)-1; i += 2 {
		ch := binary.LittleEndian.Uint16(data[i:])
		if ch == 0 {
			break
		}
		result = append(result, ch)

		if len(result)*2 > d.options.MaxStringLength {
			return "", fmt.Errorf("string: exceeds maximum length %d", d.options.MaxStringLength)
		}
	}

	return string(utf16.Decode(result)), nil
}

func (d *decoder) readStringSlice(data []byte, count uint64) ([]string, error) {
	const offsetSize = 4
	if int(count)*offsetSize > len(data) {
		return nil, fmt.Errorf("string array: offsets exceed available data")
	}

	strings := make([]string, count)
	for i := range strings {
		offset := uint64(binary.LittleEndian.Uint32(data[i*offsetSize:]))
		str, err := d.readString(offset)
		if err != nil {
			return nil, fmt.Errorf("string array: reading string at index %d: %w", i, err)
		}
		strings[i] = str
	}

	return strings, nil
}

// codec is the single owner of how a FieldType is decoded: its scalar form,
// its array-element form, and the element size used for both decoding and
// bounds checking. Types with a nil slice func (longid) decode to an untyped
// empty slice when empty and error otherwise.
type codec struct {
	scalar func(d *decoder, data []byte) (interface{}, error)
	slice  func(d *decoder, data []byte, count uint64) (interface{}, error)
}

var codecs = map[FieldType]codec{
	TypeBool:       fixedCodec(1, func(b []byte) bool { return b[0] != 0 }),
	TypeInt16:      fixedCodec(2, func(b []byte) int16 { return int16(binary.LittleEndian.Uint16(b)) }),
	TypeUint16:     fixedCodec(2, binary.LittleEndian.Uint16),
	TypeInt32:      fixedCodec(4, func(b []byte) int32 { return int32(binary.LittleEndian.Uint32(b)) }),
	TypeUint32:     fixedCodec(4, binary.LittleEndian.Uint32),
	TypeInt64:      fixedCodec(8, func(b []byte) int64 { return int64(binary.LittleEndian.Uint64(b)) }),
	TypeUint64:     fixedCodec(8, binary.LittleEndian.Uint64),
	TypeFloat32:    fixedCodec(4, func(b []byte) float32 { return math.Float32frombits(binary.LittleEndian.Uint32(b)) }),
	TypeFloat64:    fixedCodec(8, func(b []byte) float64 { return math.Float64frombits(binary.LittleEndian.Uint64(b)) }),
	TypeString:     stringCodec,
	TypeRow:        refCodec(TypeRow),
	TypeForeignRow: refCodec(TypeForeignRow),
	TypeEnumRow:    refCodec(TypeEnumRow),
	TypeLongID:     {scalar: decodeLongID},
}

// readSlice decodes count elements of size bytes each from data into a typed
// slice. A count of zero yields the empty typed slice.
func readSlice[T any](data []byte, count uint64, size int, decode func([]byte) T) ([]T, error) {
	totalSize := int(count) * size
	if totalSize > len(data) {
		return nil, fmt.Errorf("array: data exceeds available data (need %d bytes, have %d)", totalSize, len(data))
	}

	result := make([]T, count)
	for i := range result {
		result[i] = decode(data[i*size:])
	}
	return result, nil
}

func anySlice[T any](s []T, err error) (interface{}, error) {
	if err != nil {
		return nil, err
	}
	return s, nil
}

// fixedCodec builds the codec for a fixed-size type whose scalar and array
// element decode identically.
func fixedCodec[T any](size int, decode func([]byte) T) codec {
	return codec{
		scalar: func(_ *decoder, data []byte) (interface{}, error) {
			return decode(data), nil
		},
		slice: func(_ *decoder, data []byte, count uint64) (interface{}, error) {
			return anySlice(readSlice(data, count, size, decode))
		},
	}
}

var stringCodec = codec{
	scalar: func(d *decoder, data []byte) (interface{}, error) {
		return d.readString(uint64(binary.LittleEndian.Uint32(data)))
	},
	slice: func(d *decoder, data []byte, count uint64) (interface{}, error) {
		return anySlice(d.readStringSlice(data, count))
	},
}

// refCodec builds the codec for row reference types. Array elements are
// decoded as uint32 at the type's stride, but foreign/enum row arrays in
// 64-bit files reserve 16 bytes per element for bounds checking.
func refCodec(ft FieldType) codec {
	return codec{
		scalar: func(_ *decoder, data []byte) (interface{}, error) {
			value := binary.LittleEndian.Uint32(data)
			if value == NullRowSentinel {
				return nil, nil
			}
			return &value, nil
		},
		slice: func(d *decoder, data []byte, count uint64) (interface{}, error) {
			stride := ft.Size(d.width)
			elementSize := stride
			if d.width == Width64 && (ft == TypeForeignRow || ft == TypeEnumRow) {
				elementSize = ElementSize64BitForeignRow
			}

			totalSize := int(count) * elementSize
			if totalSize > len(data) {
				return nil, fmt.Errorf("array: data exceeds available data (need %d bytes, have %d)", totalSize, len(data))
			}

			return anySlice(readSlice(data[:totalSize], count, stride, decodeRowRef))
		},
	}
}

func decodeRowRef(data []byte) *uint32 {
	value := binary.LittleEndian.Uint32(data)
	if value == NullRowSentinel || value == 0 || value > MaxReasonableForeignKeyIndex {
		return nil
	}
	return &value
}

func decodeLongID(d *decoder, data []byte) (interface{}, error) {
	value := binary.LittleEndian.Uint64(data[0:8])

	if d.width == Width32 {
		if value == LongIDNullSentinel {
			return nil, nil
		}
		return &value, nil
	}

	high := binary.LittleEndian.Uint64(data[8:16])
	if value == LongIDNullSentinel && high == LongIDNullSentinel {
		return nil, nil
	}
	if high != 0 {
		return nil, fmt.Errorf("unexpected value in high half of LongID: %016x %016x", value, high)
	}
	return &value, nil
}
