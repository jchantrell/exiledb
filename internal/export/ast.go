package export

import (
	"fmt"

	"github.com/jchantrell/exiledb/internal/bundle"
)

// DecompressAST decompresses the Oodle-compressed animation payload inside an
// .ast file. Returns the file with the header intact and the payload replaced
// by the decompressed data. Files with version < 8 have uncompressed payloads
// and are returned unchanged.
func DecompressAST(data []byte) ([]byte, error) {
	if len(data) < 8 {
		return data, nil
	}

	version := data[0]
	if version < 8 {
		return data, nil
	}

	headerSize, err := astHeaderSize(data)
	if err != nil {
		return nil, fmt.Errorf("parsing AST header: %w", err)
	}

	if headerSize >= len(data) {
		return data, nil
	}

	payload, err := bundle.Decompress(data[headerSize:])
	if err != nil {
		return nil, fmt.Errorf("decompressing AST payload: %w", err)
	}

	out := make([]byte, headerSize+len(payload))
	copy(out, data[:headerSize])
	copy(out[headerSize:], payload)
	return out, nil
}

func astHeaderSize(data []byte) (int, error) {
	if len(data) < 8 {
		return 0, fmt.Errorf("file too small")
	}

	version := data[0]
	boneCount := int(data[1])
	lightCount := int(data[7])
	off := 8

	for i := 0; i < boneCount; i++ {
		if off+68 > len(data) {
			return 0, fmt.Errorf("truncated bone %d", i)
		}
		off += 2 + 64 // sibling + child + matrix
		nameLen := int(data[off])
		off++ // nameLen
		off++ // unk2
		off += nameLen
	}

	for i := 0; i < lightCount; i++ {
		if off >= len(data) {
			return 0, fmt.Errorf("truncated light %d", i)
		}
		nameLen := int(data[off])
		off++
		skipSize := 55
		if version > 8 {
			skipSize = 59
		}
		off += skipSize + nameLen
	}

	// Scan animation headers until we hit the bundle payload.
	// animCount in the header is u8 and overflows for files with >255 animations.
	for {
		if off+13 > len(data) {
			break
		}
		trackCount := int(data[off])
		framerate := int(data[off+2])
		if trackCount != boneCount || (framerate != 24 && framerate != 30 && framerate != 60) {
			break
		}
		off += 4 // trackCount + unk1 + framerate + unk2
		if version > 9 {
			off++
		}
		if off >= len(data) {
			break
		}
		nameLen := int(data[off])
		off++
		parentNameLen := 0
		if version > 10 {
			if off >= len(data) {
				break
			}
			parentNameLen = int(data[off])
			off++
		}
		off += 4 + 4 // dataOffset + dataSize
		off += nameLen + parentNameLen
	}

	return off, nil
}

