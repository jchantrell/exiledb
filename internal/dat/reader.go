package dat

import (
	"encoding/binary"
	"fmt"
	"io"
	"unicode/utf16"
	"unsafe"
)

// BinaryReader provides utilities for reading binary data from DAT files
type BinaryReader struct {
	data   []byte
	offset int
	size   int
}

// NewBinaryReader creates a new binary reader for the given data
func NewBinaryReader(data []byte) *BinaryReader {
	return &BinaryReader{
		data:   data,
		offset: 0,
		size:   len(data),
	}
}

// Position returns the current read position
func (br *BinaryReader) Position() int {
	return br.offset
}

// Size returns the total size of the data
func (br *BinaryReader) Size() int {
	return br.size
}

// Remaining returns the number of bytes remaining to read
func (br *BinaryReader) Remaining() int {
	return br.size - br.offset
}

// Seek sets the read position to the given offset
func (br *BinaryReader) Seek(offset int) error {
	if offset < 0 || offset > br.size {
		return fmt.Errorf("seek position %d out of bounds [0, %d]", offset, br.size)
	}
	br.offset = offset
	return nil
}

// Skip advances the read position by the given number of bytes
func (br *BinaryReader) Skip(bytes int) error {
	newOffset := br.offset + bytes
	if newOffset < 0 || newOffset > br.size {
		return fmt.Errorf("skip would move position to %d, out of bounds [0, %d]", newOffset, br.size)
	}
	br.offset = newOffset
	return nil
}

// ReadBytes reads the specified number of bytes and advances the position
func (br *BinaryReader) ReadBytes(count int) ([]byte, error) {
	if count < 0 {
		return nil, fmt.Errorf("cannot read negative number of bytes: %d", count)
	}

	if br.offset+count > br.size {
		return nil, fmt.Errorf("cannot read %d bytes from position %d: not enough data", count, br.offset)
	}

	result := make([]byte, count)
	copy(result, br.data[br.offset:br.offset+count])
	br.offset += count

	return result, nil
}

// PeekBytes reads bytes without advancing the position
func (br *BinaryReader) PeekBytes(count int) ([]byte, error) {
	if count < 0 {
		return nil, fmt.Errorf("cannot peek negative number of bytes: %d", count)
	}

	if br.offset+count > br.size {
		return nil, fmt.Errorf("cannot peek %d bytes from position %d: not enough data", count, br.offset)
	}

	result := make([]byte, count)
	copy(result, br.data[br.offset:br.offset+count])

	return result, nil
}

// ReadUint8 reads a single byte as uint8
func (br *BinaryReader) ReadUint8() (uint8, error) {
	if br.offset >= br.size {
		return 0, io.EOF
	}

	value := br.data[br.offset]
	br.offset++
	return value, nil
}

// ReadUint16 reads a 16-bit unsigned integer in little-endian format
func (br *BinaryReader) ReadUint16() (uint16, error) {
	if br.offset+2 > br.size {
		return 0, fmt.Errorf("cannot read uint16 from position %d: not enough data", br.offset)
	}

	value := binary.LittleEndian.Uint16(br.data[br.offset:])
	br.offset += 2
	return value, nil
}

// ReadUint32 reads a 32-bit unsigned integer in little-endian format
func (br *BinaryReader) ReadUint32() (uint32, error) {
	if br.offset+4 > br.size {
		return 0, fmt.Errorf("cannot read uint32 from position %d: not enough data", br.offset)
	}

	value := binary.LittleEndian.Uint32(br.data[br.offset:])
	br.offset += 4
	return value, nil
}

// ReadUint64 reads a 64-bit unsigned integer in little-endian format
func (br *BinaryReader) ReadUint64() (uint64, error) {
	if br.offset+8 > br.size {
		return 0, fmt.Errorf("cannot read uint64 from position %d: not enough data", br.offset)
	}

	value := binary.LittleEndian.Uint64(br.data[br.offset:])
	br.offset += 8
	return value, nil
}

// ReadInt16 reads a 16-bit signed integer in little-endian format
func (br *BinaryReader) ReadInt16() (int16, error) {
	value, err := br.ReadUint16()
	return int16(value), err
}

// ReadInt32 reads a 32-bit signed integer in little-endian format
func (br *BinaryReader) ReadInt32() (int32, error) {
	value, err := br.ReadUint32()
	return int32(value), err
}

// ReadInt64 reads a 64-bit signed integer in little-endian format
func (br *BinaryReader) ReadInt64() (int64, error) {
	value, err := br.ReadUint64()
	return int64(value), err
}

// ReadFloat32 reads a 32-bit float in little-endian format
func (br *BinaryReader) ReadFloat32() (float32, error) {
	if br.offset+4 > br.size {
		return 0, fmt.Errorf("cannot read float32 from position %d: not enough data", br.offset)
	}

	bits := binary.LittleEndian.Uint32(br.data[br.offset:])
	value := *(*float32)(unsafe.Pointer(&bits))
	br.offset += 4
	return value, nil
}

// ReadFloat64 reads a 64-bit float in little-endian format
func (br *BinaryReader) ReadFloat64() (float64, error) {
	if br.offset+8 > br.size {
		return 0, fmt.Errorf("cannot read float64 from position %d: not enough data", br.offset)
	}

	bits := binary.LittleEndian.Uint64(br.data[br.offset:])
	value := *(*float64)(unsafe.Pointer(&bits))
	br.offset += 8
	return value, nil
}

// ReadBool reads a boolean value (single byte, 0 = false, non-zero = true)
func (br *BinaryReader) ReadBool() (bool, error) {
	value, err := br.ReadUint8()
	return value != 0, err
}

// ReadNullableUint32 reads a 32-bit unsigned integer that may be null (0xfefe_fefe)
func (br *BinaryReader) ReadNullableUint32() (*uint32, error) {
	value, err := br.ReadUint32()
	if err != nil {
		return nil, err
	}

	if value == NullRowSentinel {
		return nil, nil
	}

	return &value, nil
}

// StringReader provides utilities for reading strings from DAT dynamic data
type StringReader struct {
	data []byte
}

// NewStringReader creates a new string reader for dynamic data
func NewStringReader(dynamicData []byte) *StringReader {
	return &StringReader{
		data: dynamicData,
	}
}

// ReadUTF16String reads a null-terminated UTF-16 string from the given offset
func (sr *StringReader) ReadUTF16String(offset uint64) (string, error) {
	if offset < 8 {
		return "", fmt.Errorf("string offset %d is too small (minimum 8)", offset)
	}

	if offset >= uint64(len(sr.data)) {
		return "", fmt.Errorf("string offset %d exceeds data size %d", offset, len(sr.data))
	}

	// Read UTF-16 code units until null terminator
	data := sr.data[offset:]
	var codeUnits []uint16

	for i := 0; i < len(data)-1; i += 2 {
		// Read 16-bit code unit in little-endian
		codeUnit := binary.LittleEndian.Uint16(data[i:])

		// Stop at null terminator
		if codeUnit == 0 {
			break
		}

		codeUnits = append(codeUnits, codeUnit)
	}

	// Convert UTF-16 to Go string (UTF-8)
	runes := utf16.Decode(codeUnits)
	return string(runes), nil
}

// ReadUTF32String reads a null-terminated UTF-32 string from the given offset
func (sr *StringReader) ReadUTF32String(offset uint64) (string, error) {
	if offset < 8 {
		return "", fmt.Errorf("string offset %d is too small (minimum 8)", offset)
	}

	if offset >= uint64(len(sr.data)) {
		return "", fmt.Errorf("string offset %d exceeds data size %d", offset, len(sr.data))
	}

	// Read UTF-32 runes until null terminator
	data := sr.data[offset:]
	var runes []rune

	for i := 0; i < len(data)-3; i += 4 {
		// Read 32-bit rune in little-endian
		runeValue := binary.LittleEndian.Uint32(data[i:])

		// Stop at null terminator
		if runeValue == 0 {
			break
		}

		// Validate rune value
		if runeValue > 0x10FFFF {
			return "", fmt.Errorf("invalid UTF-32 rune value: 0x%X", runeValue)
		}

		runes = append(runes, rune(runeValue))
	}

	return string(runes), nil
}

// ArrayReader provides utilities for reading arrays from DAT dynamic data
type ArrayReader struct {
	data         []byte
	stringReader *StringReader
}

// NewArrayReader creates a new array reader for dynamic data
func NewArrayReader(dynamicData []byte) *ArrayReader {
	return &ArrayReader{
		data:         dynamicData,
		stringReader: NewStringReader(dynamicData),
	}
}

// ReadArrayMetadata reads array count and offset from fixed row data
func ReadArrayMetadata(rowData []byte) (count uint64, offset uint64, err error) {
	if len(rowData) < 16 {
		return 0, 0, fmt.Errorf("insufficient data for array metadata: need 16 bytes, have %d", len(rowData))
	}

	count = binary.LittleEndian.Uint64(rowData[0:8])
	offset = binary.LittleEndian.Uint64(rowData[8:16])

	return count, offset, nil
}

// ReadTypedArray reads an array of the specified type from the given offset
func (ar *ArrayReader) ReadTypedArray(offset uint64, count uint64, elementType FieldType) (interface{}, error) {
	if offset < 8 {
		return nil, fmt.Errorf("array offset %d is too small (minimum 8)", offset)
	}

	if offset >= uint64(len(ar.data)) {
		return nil, fmt.Errorf("array offset %d exceeds data size %d", offset, len(ar.data))
	}

	if count == 0 {
		// Return empty slice of appropriate type
		return ar.createEmptyTypedSlice(elementType), nil
	}

	data := ar.data[offset:]
	elementSize := elementType.Size()

	// Special handling for string arrays
	if elementType == TypeString {
		return ar.readStringArray(data, count)
	}

	// Calculate total bytes needed
	totalBytes := int(count) * elementSize
	if totalBytes > len(data) {
		return nil, fmt.Errorf("array data exceeds available data: need %d bytes, have %d", totalBytes, len(data))
	}

	// Read the typed array
	return ar.readPrimitiveArray(data[:totalBytes], count, elementType)
}

// createEmptyTypedSlice creates an empty slice of the appropriate type
func (ar *ArrayReader) createEmptyTypedSlice(elementType FieldType) interface{} {
	switch elementType {
	case TypeBool:
		return []bool{}
	case TypeString:
		return []string{}
	case TypeInt16:
		return []int16{}
	case TypeUint16:
		return []uint16{}
	case TypeInt32:
		return []int32{}
	case TypeUint32:
		return []uint32{}
	case TypeInt64:
		return []int64{}
	case TypeUint64:
		return []uint64{}
	case TypeFloat32:
		return []float32{}
	case TypeFloat64:
		return []float64{}
	case TypeRow, TypeForeignRow, TypeEnumRow:
		return []*uint32{}
	default:
		return []interface{}{}
	}
}

// readStringArray reads an array of string offsets and returns the actual strings
func (ar *ArrayReader) readStringArray(data []byte, count uint64) ([]string, error) {
	// Each string offset is 8 bytes
	offsetSize := 8
	totalOffsetBytes := int(count) * offsetSize

	if totalOffsetBytes > len(data) {
		return nil, fmt.Errorf("string array offsets exceed available data: need %d bytes, have %d", totalOffsetBytes, len(data))
	}

	strings := make([]string, count)
	for i := uint64(0); i < count; i++ {
		offsetData := data[i*8 : (i+1)*8]
		offset := binary.LittleEndian.Uint64(offsetData)

		str, err := ar.stringReader.ReadUTF16String(offset)
		if err != nil {
			return nil, fmt.Errorf("reading string at index %d: %w", i, err)
		}
		strings[i] = str
	}

	return strings, nil
}

// readPrimitiveArray reads an array of primitive values
func (ar *ArrayReader) readPrimitiveArray(data []byte, count uint64, elementType FieldType) (interface{}, error) {
	switch elementType {
	case TypeBool:
		result := make([]bool, count)
		for i := uint64(0); i < count; i++ {
			result[i] = data[i] != 0
		}
		return result, nil

	case TypeInt16:
		result := make([]int16, count)
		for i := uint64(0); i < count; i++ {
			offset := i * 2
			result[i] = int16(binary.LittleEndian.Uint16(data[offset:]))
		}
		return result, nil

	case TypeUint16:
		result := make([]uint16, count)
		for i := uint64(0); i < count; i++ {
			offset := i * 2
			result[i] = binary.LittleEndian.Uint16(data[offset:])
		}
		return result, nil

	case TypeInt32:
		result := make([]int32, count)
		for i := uint64(0); i < count; i++ {
			offset := i * 4
			result[i] = int32(binary.LittleEndian.Uint32(data[offset:]))
		}
		return result, nil

	case TypeUint32:
		result := make([]uint32, count)
		for i := uint64(0); i < count; i++ {
			offset := i * 4
			result[i] = binary.LittleEndian.Uint32(data[offset:])
		}
		return result, nil

	case TypeInt64:
		result := make([]int64, count)
		for i := uint64(0); i < count; i++ {
			offset := i * 8
			result[i] = int64(binary.LittleEndian.Uint64(data[offset:]))
		}
		return result, nil

	case TypeUint64:
		result := make([]uint64, count)
		for i := uint64(0); i < count; i++ {
			offset := i * 8
			result[i] = binary.LittleEndian.Uint64(data[offset:])
		}
		return result, nil

	case TypeFloat32:
		result := make([]float32, count)
		for i := uint64(0); i < count; i++ {
			offset := i * 4
			bits := binary.LittleEndian.Uint32(data[offset:])
			result[i] = *(*float32)(unsafe.Pointer(&bits))
		}
		return result, nil

	case TypeFloat64:
		result := make([]float64, count)
		for i := uint64(0); i < count; i++ {
			offset := i * 8
			bits := binary.LittleEndian.Uint64(data[offset:])
			result[i] = *(*float64)(unsafe.Pointer(&bits))
		}
		return result, nil

	case TypeRow, TypeForeignRow, TypeEnumRow:
		result := make([]*uint32, count)
		for i := uint64(0); i < count; i++ {
			offset := i * 4
			value := binary.LittleEndian.Uint32(data[offset:])
			if value == NullRowSentinel {
				result[i] = nil
			} else {
				result[i] = &value
			}
		}
		return result, nil

	default:
		return nil, fmt.Errorf("unsupported array element type: %s", elementType)
	}
}

// DataValidator provides utilities for validating DAT file data
type DataValidator struct {
	options *ValidationOptions
}

// ValidationOptions configures data validation behavior
type ValidationOptions struct {
	// MaxStringLength is the maximum allowed string length
	MaxStringLength int

	// MaxArrayCount is the maximum allowed array element count
	MaxArrayCount int

	// ValidateStringOffsets checks that string offsets are within bounds
	ValidateStringOffsets bool

	// ValidateArrayOffsets checks that array offsets are within bounds
	ValidateArrayOffsets bool
}

// DefaultValidationOptions returns sensible default validation options
func DefaultValidationOptions() *ValidationOptions {
	return &ValidationOptions{
		MaxStringLength:       65536, // 64KB
		MaxArrayCount:         65536, // 64K elements
		ValidateStringOffsets: true,
		ValidateArrayOffsets:  true,
	}
}

// NewDataValidator creates a new data validator with the given options
func NewDataValidator(options *ValidationOptions) *DataValidator {
	if options == nil {
		options = DefaultValidationOptions()
	}

	return &DataValidator{
		options: options,
	}
}

// ValidateStringOffset validates that a string offset is within valid bounds
func (dv *DataValidator) ValidateStringOffset(offset uint64, dataSize int) error {
	if !dv.options.ValidateStringOffsets {
		return nil
	}

	if offset < 8 {
		return fmt.Errorf("string offset %d is too small (minimum 8)", offset)
	}

	if offset >= uint64(dataSize) {
		return fmt.Errorf("string offset %d exceeds data size %d", offset, dataSize)
	}

	return nil
}

// ValidateArrayOffset validates that an array offset is within valid bounds
func (dv *DataValidator) ValidateArrayOffset(offset uint64, dataSize int) error {
	if !dv.options.ValidateArrayOffsets {
		return nil
	}

	if offset < 8 {
		return fmt.Errorf("array offset %d is too small (minimum 8)", offset)
	}

	if offset >= uint64(dataSize) {
		return fmt.Errorf("array offset %d exceeds data size %d", offset, dataSize)
	}

	return nil
}

// ValidateArrayCount validates that an array count is within reasonable limits
func (dv *DataValidator) ValidateArrayCount(count uint64) error {
	if count > uint64(dv.options.MaxArrayCount) {
		return fmt.Errorf("array count %d exceeds maximum %d", count, dv.options.MaxArrayCount)
	}

	return nil
}

// ValidateStringLength validates that a string length is within reasonable limits
func (dv *DataValidator) ValidateStringLength(length int) error {
	if length > dv.options.MaxStringLength {
		return fmt.Errorf("string length %d exceeds maximum %d", length, dv.options.MaxStringLength)
	}

	return nil
}

