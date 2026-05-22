package export

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/oriath-net/gooz"
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

	payload, err := decompressBundle(data[headerSize:])
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
	animCount := int(data[3])
	lightCount := int(data[7])
	off := 8

	for i := 0; i < boneCount; i++ {
		if off+67 > len(data) {
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

	for i := 0; i < animCount; i++ {
		if off >= len(data) {
			return 0, fmt.Errorf("truncated anim header %d", i)
		}
		off += 4 // trackCount + unk1 + framerate + unk2
		if version > 9 {
			off++ // v11a
		}
		if off >= len(data) {
			return 0, fmt.Errorf("truncated anim header %d", i)
		}
		nameLen := int(data[off])
		off++
		parentNameLen := 0
		if version > 10 {
			if off >= len(data) {
				return 0, fmt.Errorf("truncated anim header %d", i)
			}
			parentNameLen = int(data[off])
			off++
		}
		off += 4 + 4 // dataOffset + dataSize
		off += nameLen + parentNameLen
	}

	return off, nil
}

type bundleHeader struct {
	UncompressedSize  uint32
	TotalPayloadSize  uint32
	HeadPayloadSize   uint32
	Compression       uint32
	_                 uint32
	UncompressedSize2 int64
	TotalPayloadSize2 int64
	BlockCount        uint32
	Granularity       uint32
	_                 [4]uint32
}

func decompressBundle(data []byte) ([]byte, error) {
	r := bytes.NewReader(data)

	var hdr bundleHeader
	if err := binary.Read(r, binary.LittleEndian, &hdr); err != nil {
		return nil, fmt.Errorf("reading bundle header: %w", err)
	}

	blockSizes := make([]uint32, hdr.BlockCount)
	if err := binary.Read(r, binary.LittleEndian, &blockSizes); err != nil {
		return nil, fmt.Errorf("reading block sizes: %w", err)
	}

	uncompressedSize := int(hdr.UncompressedSize)
	granularity := int(hdr.Granularity)
	if granularity == 0 {
		return nil, fmt.Errorf("granularity is 0")
	}

	result := make([]byte, uncompressedSize)
	outBuf := make([]byte, granularity+64)
	offset := 0

	for i, blockSize := range blockSizes {
		block := make([]byte, blockSize)
		if _, err := io.ReadFull(r, block); err != nil {
			return nil, fmt.Errorf("reading block %d: %w", i, err)
		}

		decompSize := granularity
		if i == len(blockSizes)-1 {
			decompSize = uncompressedSize - granularity*(len(blockSizes)-1)
		}

		if _, err := gooz.Decompress(block, outBuf[:decompSize]); err != nil {
			return nil, fmt.Errorf("decompressing block %d: %w", i, err)
		}

		copy(result[offset:], outBuf[:decompSize])
		offset += decompSize
	}

	return result, nil
}
