package bundle

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

type bundleIndex struct {
	bundles []string
	files   []bundleFileInfo
}

type bundleFileInfo struct {
	path     string
	bundleId uint32
	offset   uint32
	size     uint32
}

type bundlePathrep struct {
	offset        uint32
	size          uint32
	recursiveSize uint32
}

func loadBundleIndex(indexFile io.ReaderAt) (bundleIndex, error) {
	// Try to determine if this is compressed bundle data or raw index data
	// by attempting to read it as a bundle first
	indexBundle, err := OpenBundle(indexFile)
	if err != nil {
		// If it fails to parse as a bundle, assume it's raw index data
		return loadBundleIndexFromRawData(indexFile)
	}

	// Successfully parsed as bundle - decompress it
	indexData := make([]byte, indexBundle.Size())
	if _, err := indexBundle.ReadAt(indexData, 0); err != nil {
		return bundleIndex{}, fmt.Errorf("unable to read index bundle: %w", err)
	}

	// Parse the decompressed data
	return loadBundleIndexFromRawData(bytes.NewReader(indexData))
}

func loadBundleIndexFromRawData(indexFile io.ReaderAt) (bundleIndex, error) {
	// Read all the raw index data
	var indexData []byte
	var offset int64 = 0
	buf := make([]byte, 64*1024) // 64KB chunks

	for {
		n, err := indexFile.ReadAt(buf, offset)
		if err != nil && err != io.EOF {
			return bundleIndex{}, fmt.Errorf("unable to read index data: %w", err)
		}
		indexData = append(indexData, buf[:n]...)
		if err == io.EOF || n == 0 {
			break
		}
		offset += int64(n)
	}

	p := 0

	// Check if we have enough data to read at least the bundle count
	if len(indexData) < 4 {
		return bundleIndex{}, fmt.Errorf("index data too small: got %d bytes, need at least 4", len(indexData))
	}

	bundleCount := binary.LittleEndian.Uint32(indexData[p:])
	p += 4

	bundles := make([]string, bundleCount)
	for i := range bundles {
		nameLen := int(binary.LittleEndian.Uint32(indexData[p:]))
		p += 4

		name := string(indexData[p : p+nameLen])
		p += nameLen

		// skip uncompressed size -- available elsewhere
		p += 4

		bundles[i] = name
	}

	fileCount := binary.LittleEndian.Uint32(indexData[p:])
	p += 4

	files := make([]bundleFileInfo, fileCount)
	filemap := make(map[uint64]int, fileCount)
	for i := 0; i < int(fileCount); i++ {
		hash := binary.LittleEndian.Uint64(indexData[p+0:])
		files[i] = bundleFileInfo{
			bundleId: binary.LittleEndian.Uint32(indexData[p+8:]),
			offset:   binary.LittleEndian.Uint32(indexData[p+12:]),
			size:     binary.LittleEndian.Uint32(indexData[p+16:]),
		}
		p += 20
		if _, exists := filemap[hash]; exists {
			panic("duplicate filemap hash")
		}
		filemap[hash] = i
	}

	pathrepCount := binary.LittleEndian.Uint32(indexData[p:])
	p += 4

	pathmap := make(map[uint64]bundlePathrep, pathrepCount)
	for i := uint32(0); i < pathrepCount; i++ {
		hash := binary.LittleEndian.Uint64(indexData[p+0:])
		pr := bundlePathrep{
			offset:        binary.LittleEndian.Uint32(indexData[p+8:]),
			size:          binary.LittleEndian.Uint32(indexData[p+12:]),
			recursiveSize: binary.LittleEndian.Uint32(indexData[p+16:]),
		}
		p += 20
		if _, exists := pathmap[hash]; exists {
			panic("duplicate pathmap hash")
		}
		pathmap[hash] = pr
	}

	if p >= len(indexData) {
		return bundleIndex{}, fmt.Errorf("pathrep bundle offset %d exceeds data length %d", p, len(indexData))
	}

	pathrepBundle, err := OpenBundle(bytes.NewReader(indexData[p:]))
	if err != nil {
		return bundleIndex{}, fmt.Errorf("unable to read pathrep bundle at offset %d: %w", p, err)
	}

	pathData := make([]byte, pathrepBundle.Size())
	if _, err := pathrepBundle.ReadAt(pathData, 0); err != nil {
		return bundleIndex{}, fmt.Errorf("unable to read pathrep bundle: %w", err)
	}

	for _, pr := range pathmap {
		data := pathData[pr.offset : pr.offset+pr.size]
		paths := readPathspec(data)
		for _, path := range paths {
			// Try modern hash first (MurmurHash64A for PoE ≥3.21.2)
			modernHash := MurmurHashPath(path)
			if fe, found := filemap[modernHash]; found {
				files[fe].path = path
			} else {
				// Fallback to legacy hash (FNV1a for PoE ≤3.21.2)
				legacyHash := FNVHashPath(path)
				if fe, found := filemap[legacyHash]; found {
					files[fe].path = path
				} else {
					// This is not a panic condition - some paths in the pathmap
					// may not have corresponding files in the filemap
					// This is normal behavior for bundle indices
					continue
				}
			}
		}
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].path < files[j].path
	})

	return bundleIndex{
		bundles: bundles,
		files:   files,
	}, nil
}

func readPathspec(data []byte) []string {
	p := int(0)
	phase := 1
	names := make([]string, 0, 128)
	output := make([]string, 0, 128)

	for p < len(data) {
		n := int(binary.LittleEndian.Uint32(data[p:]))
		p += 4
		if n == 0 {
			phase = 1 - phase
			continue
		}

		str := readPathspecString(data, &p)
		if n-1 < len(names) {
			str = names[n-1] + str
		}
		if phase == 0 {
			names = append(names, str)
		} else {
			output = append(output, str)
		}
	}

	return output
}

// GetFileInfo finds a file in the index and returns its location info

func readPathspecString(data []byte, offset *int) string {
	p := *offset
	for p < len(data) && data[p] != 0 {
		p++
	}
	s := string(data[*offset:p])
	*offset = p + 1
	return s
}

// LoadIndex parses index data and returns an Index interface
func LoadIndex(data []byte) (Index, error) {
	reader := bytes.NewReader(data)
	internal, err := loadBundleIndex(reader)
	if err != nil {
		return nil, fmt.Errorf("loading bundle index: %w", err)
	}

	return &indexImpl{internal: internal}, nil
}

// indexImpl is a concrete implementation of the Index interface
type indexImpl struct {
	internal bundleIndex
}

// GetFileInfo returns information about a file, including which bundle contains it
func (idx *indexImpl) GetFileInfo(path string) (*FileLocation, error) {
	files := idx.internal.files

	// Binary search for the file
	i := sort.Search(len(files), func(i int) bool {
		return files[i].path >= path
	})

	if i < len(files) && files[i].path == path {
		file := &files[i]
		return &FileLocation{
			BundleName: idx.internal.bundles[file.bundleId],
			Offset:     file.offset,
			Size:       file.size,
		}, nil
	}

	return nil, fmt.Errorf("file not found: %s", path)
}

// ListBundles returns all bundle names in the index
func (idx *indexImpl) ListBundles() []string {
	return idx.internal.bundles
}

// ListFiles returns all file paths in the index
func (idx *indexImpl) ListFiles() []string {
	files := make([]string, len(idx.internal.files))
	for i, file := range idx.internal.files {
		files[i] = file.path
	}
	return files
}
