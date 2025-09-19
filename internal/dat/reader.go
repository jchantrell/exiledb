package dat

import (
	"encoding/binary"
	"fmt"
	"unicode/utf16"
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

// ReadArrayMetadata reads array count and offset from fixed row data
func ReadArrayMetadata(rowData []byte) (count uint64, offset uint64, err error) {
	if len(rowData) < 16 {
		return 0, 0, fmt.Errorf("insufficient data for array metadata: need 16 bytes, have %d", len(rowData))
	}

	count = binary.LittleEndian.Uint64(rowData[0:8])
	offset = binary.LittleEndian.Uint64(rowData[8:16])

	return count, offset, nil
}


