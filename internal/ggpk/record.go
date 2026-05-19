package ggpk

import (
	"encoding/binary"
	"fmt"
	"io"
	"unicode/utf16"
)

type RecordTag uint32

const (
	TagGGPK RecordTag = 0x4B504747
	TagPDIR RecordTag = 0x52494450
	TagFILE RecordTag = 0x454C4946
	TagFREE RecordTag = 0x45455246
)

type RecordHeader struct {
	Length uint32
	Tag    RecordTag
}

type GgpkRecord struct {
	Version    uint32
	RootOffset uint64
	FreeOffset uint64
}

type DirectoryRecord struct {
	Offset  uint64
	Name    string
	Hash    [32]byte
	Entries []DirectoryEntry
}

type DirectoryEntry struct {
	NameHash uint32
	Offset   uint64
}

type FileRecord struct {
	Offset     uint64
	Name       string
	Hash       [32]byte
	DataOffset uint64
	DataLength uint64
}

const recordHeaderSize = 8

func readRecordHeader(r io.ReaderAt, offset uint64) (RecordHeader, error) {
	var buf [recordHeaderSize]byte
	if _, err := r.ReadAt(buf[:], int64(offset)); err != nil {
		return RecordHeader{}, fmt.Errorf("reading record header at %d: %w", offset, err)
	}
	return RecordHeader{
		Length: binary.LittleEndian.Uint32(buf[0:4]),
		Tag:    RecordTag(binary.LittleEndian.Uint32(buf[4:8])),
	}, nil
}

func readGgpkRecord(r io.ReaderAt, offset uint64) (GgpkRecord, error) {
	var buf [28]byte
	if _, err := r.ReadAt(buf[:], int64(offset)); err != nil {
		return GgpkRecord{}, fmt.Errorf("reading GGPK record: %w", err)
	}
	return GgpkRecord{
		Version:    binary.LittleEndian.Uint32(buf[8:12]),
		RootOffset: binary.LittleEndian.Uint64(buf[12:20]),
		FreeOffset: binary.LittleEndian.Uint64(buf[20:28]),
	}, nil
}

func readDirectoryRecord(r io.ReaderAt, offset uint64, version uint32) (DirectoryRecord, error) {
	var fixedBuf [48]byte
	if _, err := r.ReadAt(fixedBuf[:], int64(offset)); err != nil {
		return DirectoryRecord{}, fmt.Errorf("reading directory record at %d: %w", offset, err)
	}

	nameLen := binary.LittleEndian.Uint32(fixedBuf[8:12])
	entryCount := binary.LittleEndian.Uint32(fixedBuf[12:16])

	var hash [32]byte
	copy(hash[:], fixedBuf[16:48])

	actualNameLen := nameLen - 1
	var nameByteSize, nullTermSize uint32
	if version >= 4 {
		nameByteSize = actualNameLen * 4
		nullTermSize = 4
	} else {
		nameByteSize = actualNameLen * 2
		nullTermSize = 2
	}

	variableSize := nameByteSize + nullTermSize + entryCount*12
	variableBuf := make([]byte, variableSize)
	if _, err := r.ReadAt(variableBuf, int64(offset)+48); err != nil {
		return DirectoryRecord{}, fmt.Errorf("reading directory variable data at %d: %w", offset, err)
	}

	name := decodeString(variableBuf[:nameByteSize], version)

	entriesStart := nameByteSize + nullTermSize
	entries := make([]DirectoryEntry, entryCount)
	for i := uint32(0); i < entryCount; i++ {
		off := entriesStart + i*12
		entries[i] = DirectoryEntry{
			NameHash: binary.LittleEndian.Uint32(variableBuf[off : off+4]),
			Offset:   binary.LittleEndian.Uint64(variableBuf[off+4 : off+12]),
		}
	}

	return DirectoryRecord{
		Offset:  offset,
		Name:    name,
		Hash:    hash,
		Entries: entries,
	}, nil
}

func readFileRecord(r io.ReaderAt, offset uint64, version uint32) (FileRecord, error) {
	var fixedBuf [44]byte
	if _, err := r.ReadAt(fixedBuf[:], int64(offset)); err != nil {
		return FileRecord{}, fmt.Errorf("reading file record at %d: %w", offset, err)
	}

	length := binary.LittleEndian.Uint32(fixedBuf[0:4])
	nameLen := binary.LittleEndian.Uint32(fixedBuf[8:12])

	var hash [32]byte
	copy(hash[:], fixedBuf[12:44])

	actualNameLen := nameLen - 1
	var nameByteSize, nullTermSize uint32
	if version >= 4 {
		nameByteSize = actualNameLen * 4
		nullTermSize = 4
	} else {
		nameByteSize = actualNameLen * 2
		nullTermSize = 2
	}

	nameBuf := make([]byte, nameByteSize+nullTermSize)
	if _, err := r.ReadAt(nameBuf, int64(offset)+44); err != nil {
		return FileRecord{}, fmt.Errorf("reading file name at %d: %w", offset, err)
	}

	name := decodeString(nameBuf[:nameByteSize], version)

	headerEnd := uint64(44) + uint64(nameByteSize) + uint64(nullTermSize)
	dataOffset := offset + headerEnd
	dataLength := uint64(length) - headerEnd

	return FileRecord{
		Offset:     offset,
		Name:       name,
		Hash:       hash,
		DataOffset: dataOffset,
		DataLength: dataLength,
	}, nil
}

func decodeString(data []byte, version uint32) string {
	if version >= 4 {
		runes := make([]rune, len(data)/4)
		for i := range runes {
			runes[i] = rune(binary.LittleEndian.Uint32(data[i*4 : i*4+4]))
		}
		return string(runes)
	}
	u16s := make([]uint16, len(data)/2)
	for i := range u16s {
		u16s[i] = binary.LittleEndian.Uint16(data[i*2 : i*2+2])
	}
	return string(utf16.Decode(u16s))
}
