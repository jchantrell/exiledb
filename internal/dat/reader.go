package dat

import (
	"encoding/binary"
	"fmt"
	"unicode/utf16"
	"unsafe"
)


// ReadUTF16String reads a null-terminated UTF-16 string from the given offset in data
func ReadUTF16String(data []byte, offset uint64) (string, error) {
	if offset < 8 {
		return "", fmt.Errorf("string offset %d is too small (minimum 8)", offset)
	}

	if offset >= uint64(len(data)) {
		return "", fmt.Errorf("string offset %d exceeds data size %d", offset, len(data))
	}

	// Read UTF-16 code units until null terminator
	stringData := data[offset:]
	var codeUnits []uint16

	for i := 0; i < len(stringData)-1; i += 2 {
		// Read 16-bit code unit in little-endian
		codeUnit := binary.LittleEndian.Uint16(stringData[i:])

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

// ReadUTF32String reads a null-terminated UTF-32 string from the given offset in data
func ReadUTF32String(data []byte, offset uint64) (string, error) {
	if offset < 8 {
		return "", fmt.Errorf("string offset %d is too small (minimum 8)", offset)
	}

	if offset >= uint64(len(data)) {
		return "", fmt.Errorf("string offset %d exceeds data size %d", offset, len(data))
	}

	// Read UTF-32 runes until null terminator
	stringData := data[offset:]
	var runes []rune

	for i := 0; i < len(stringData)-3; i += 4 {
		// Read 32-bit rune in little-endian
		runeValue := binary.LittleEndian.Uint32(stringData[i:])

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
	data []byte
}

// NewArrayReader creates a new array reader for dynamic data
func NewArrayReader(dynamicData []byte) *ArrayReader {
	return &ArrayReader{
		data: dynamicData,
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

		str, err := ReadUTF16String(ar.data, offset)
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

