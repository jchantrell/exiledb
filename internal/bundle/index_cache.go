package bundle

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
)

const (
	cacheMagic   = "EIDX"
	cacheVersion = uint32(2)
)

// LoadIndexCached parses compressed index data, using an on-disk cache at
// cachePath (if non-empty) to skip the parse when the source is unchanged.
func LoadIndexCached(compressedData []byte, cachePath string) (*Index, error) {
	sourceLen := int64(len(compressedData))

	if cachePath != "" {
		idx, err := readIndexCache(cachePath, sourceLen)
		if err == nil {
			slog.Debug("Loaded index from cache", "path", cachePath)
			return idx, nil
		}
		slog.Debug("Index cache miss", "path", cachePath, "reason", err)
	}

	idx, err := loadBundleIndex(bytes.NewReader(compressedData))
	if err != nil {
		return nil, err
	}

	if cachePath != "" {
		if err := writeIndexCache(cachePath, sourceLen, idx); err != nil {
			slog.Debug("Failed to write index cache", "path", cachePath, "error", err)
		}
	}

	return idx, nil
}

func readIndexCache(cachePath string, expectedSourceLen int64) (*Index, error) {
	f, err := os.Open(cachePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var header [16]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		return nil, fmt.Errorf("reading header: %w", err)
	}
	if string(header[0:4]) != cacheMagic {
		return nil, fmt.Errorf("invalid cache magic")
	}
	if binary.LittleEndian.Uint32(header[4:8]) != cacheVersion {
		return nil, fmt.Errorf("cache version mismatch")
	}
	if int64(binary.LittleEndian.Uint64(header[8:16])) != expectedSourceLen {
		return nil, fmt.Errorf("source data changed")
	}

	r := bufio.NewReaderSize(f, 256*1024)
	var buf [8]byte

	if _, err := io.ReadFull(r, buf[:4]); err != nil {
		return nil, err
	}
	bundleCount := int(binary.LittleEndian.Uint32(buf[:4]))

	bundles := make([]string, bundleCount)
	for i := range bundles {
		if _, err := io.ReadFull(r, buf[:4]); err != nil {
			return nil, err
		}
		nameLen := binary.LittleEndian.Uint32(buf[:4])
		name := make([]byte, nameLen)
		if _, err := io.ReadFull(r, name); err != nil {
			return nil, err
		}
		bundles[i] = string(name)
	}

	if _, err := io.ReadFull(r, buf[:4]); err != nil {
		return nil, err
	}
	fileCount := int(binary.LittleEndian.Uint32(buf[:4]))

	if _, err := io.ReadFull(r, buf[:8]); err != nil {
		return nil, err
	}
	pathBlobSize := binary.LittleEndian.Uint64(buf[:8])

	pathBlob := make([]byte, pathBlobSize)
	if _, err := io.ReadFull(r, pathBlob); err != nil {
		return nil, err
	}

	files := make([]bundleFileInfo, fileCount)
	pos := 0
	for i := range files {
		end := pos
		for end < len(pathBlob) && pathBlob[end] != 0 {
			end++
		}
		files[i].path = string(pathBlob[pos:end])
		pos = end + 1
	}

	metaBlob := make([]byte, fileCount*12)
	if _, err := io.ReadFull(r, metaBlob); err != nil {
		return nil, err
	}
	for i := range files {
		off := i * 12
		files[i].bundleId = binary.LittleEndian.Uint32(metaBlob[off:])
		files[i].offset = binary.LittleEndian.Uint32(metaBlob[off+4:])
		files[i].size = binary.LittleEndian.Uint32(metaBlob[off+8:])
		if files[i].bundleId >= uint32(len(bundles)) {
			return nil, fmt.Errorf("file record %d references bundle %d of %d", i, files[i].bundleId, len(bundles))
		}
	}

	return &Index{bundles: bundles, files: files}, nil
}

func writeIndexCache(cachePath string, sourceLen int64, idx *Index) error {
	tmpPath := cachePath + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	w := bufio.NewWriterSize(f, 1024*1024)

	var header [16]byte
	copy(header[0:4], cacheMagic)
	binary.LittleEndian.PutUint32(header[4:8], cacheVersion)
	binary.LittleEndian.PutUint64(header[8:16], uint64(sourceLen))
	w.Write(header[:])

	var buf [8]byte

	binary.LittleEndian.PutUint32(buf[:4], uint32(len(idx.bundles)))
	w.Write(buf[:4])
	for _, name := range idx.bundles {
		binary.LittleEndian.PutUint32(buf[:4], uint32(len(name)))
		w.Write(buf[:4])
		w.WriteString(name)
	}

	binary.LittleEndian.PutUint32(buf[:4], uint32(len(idx.files)))
	w.Write(buf[:4])

	pathBlobSize := uint64(0)
	for i := range idx.files {
		pathBlobSize += uint64(len(idx.files[i].path)) + 1
	}
	binary.LittleEndian.PutUint64(buf[:8], pathBlobSize)
	w.Write(buf[:8])

	for i := range idx.files {
		w.WriteString(idx.files[i].path)
		w.WriteByte(0)
	}

	for i := range idx.files {
		binary.LittleEndian.PutUint32(buf[0:4], idx.files[i].bundleId)
		w.Write(buf[:4])
		binary.LittleEndian.PutUint32(buf[0:4], idx.files[i].offset)
		w.Write(buf[:4])
		binary.LittleEndian.PutUint32(buf[0:4], idx.files[i].size)
		w.Write(buf[:4])
	}

	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	return os.Rename(tmpPath, cachePath)
}
