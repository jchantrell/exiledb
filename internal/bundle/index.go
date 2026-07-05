package bundle

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
)

// Index is the parsed bundle index: which bundles exist and where each file
// lives within them. File paths are kept case-fold sorted for binary search.
type Index struct {
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

func loadBundleIndex(indexFile io.ReaderAt) (*Index, error) {
	indexBundle, err := OpenBundle(indexFile)
	if err != nil {
		return loadBundleIndexFromRawData(indexFile)
	}

	indexData := make([]byte, indexBundle.Size())
	if _, err := indexBundle.ReadAt(indexData, 0); err != nil {
		return nil, fmt.Errorf("unable to read index bundle: %w", err)
	}

	return loadBundleIndexFromRawData(bytes.NewReader(indexData))
}

func loadBundleIndexFromRawData(indexFile io.ReaderAt) (*Index, error) {
	var indexData []byte
	var offset int64 = 0
	buf := make([]byte, 64*1024) // 64KB chunks

	for {
		n, err := indexFile.ReadAt(buf, offset)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("unable to read index data: %w", err)
		}
		indexData = append(indexData, buf[:n]...)
		if err == io.EOF || n == 0 {
			break
		}
		offset += int64(n)
	}

	cur := &byteCursor{data: indexData}

	bundleCount, err := cur.uint32()
	if err != nil {
		return nil, fmt.Errorf("reading bundle count: %w", err)
	}
	if int64(bundleCount)*8 > int64(cur.remaining()) {
		return nil, fmt.Errorf("bundle count %d exceeds index data size", bundleCount)
	}

	bundles := make([]string, bundleCount)
	for i := range bundles {
		nameLen, err := cur.uint32()
		if err != nil {
			return nil, fmt.Errorf("reading bundle %d name length: %w", i, err)
		}
		name, err := cur.take(int(nameLen))
		if err != nil {
			return nil, fmt.Errorf("reading bundle %d name: %w", i, err)
		}
		// skip uncompressed size -- available elsewhere
		if _, err := cur.take(4); err != nil {
			return nil, fmt.Errorf("reading bundle %d size: %w", i, err)
		}
		bundles[i] = string(name)
	}

	fileCount, err := cur.uint32()
	if err != nil {
		return nil, fmt.Errorf("reading file count: %w", err)
	}
	if int64(fileCount)*20 > int64(cur.remaining()) {
		return nil, fmt.Errorf("file count %d exceeds index data size", fileCount)
	}

	files := make([]bundleFileInfo, fileCount)
	filemap := make(map[uint64]int, fileCount)
	for i := 0; i < int(fileCount); i++ {
		rec, err := cur.take(20)
		if err != nil {
			return nil, fmt.Errorf("reading file record %d: %w", i, err)
		}
		hash := binary.LittleEndian.Uint64(rec[0:])
		files[i] = bundleFileInfo{
			bundleId: binary.LittleEndian.Uint32(rec[8:]),
			offset:   binary.LittleEndian.Uint32(rec[12:]),
			size:     binary.LittleEndian.Uint32(rec[16:]),
		}
		if files[i].bundleId >= bundleCount {
			return nil, fmt.Errorf("file record %d references bundle %d of %d", i, files[i].bundleId, bundleCount)
		}
		if _, exists := filemap[hash]; exists {
			return nil, fmt.Errorf("duplicate filemap hash %016x at record %d", hash, i)
		}
		filemap[hash] = i
	}

	pathrepCount, err := cur.uint32()
	if err != nil {
		return nil, fmt.Errorf("reading pathrep count: %w", err)
	}
	if int64(pathrepCount)*20 > int64(cur.remaining()) {
		return nil, fmt.Errorf("pathrep count %d exceeds index data size", pathrepCount)
	}

	pathmap := make(map[uint64]bundlePathrep, pathrepCount)
	for i := uint32(0); i < pathrepCount; i++ {
		rec, err := cur.take(20)
		if err != nil {
			return nil, fmt.Errorf("reading pathrep record %d: %w", i, err)
		}
		hash := binary.LittleEndian.Uint64(rec[0:])
		pr := bundlePathrep{
			offset:        binary.LittleEndian.Uint32(rec[8:]),
			size:          binary.LittleEndian.Uint32(rec[12:]),
			recursiveSize: binary.LittleEndian.Uint32(rec[16:]),
		}
		if _, exists := pathmap[hash]; exists {
			return nil, fmt.Errorf("duplicate pathmap hash %016x at record %d", hash, i)
		}
		pathmap[hash] = pr
	}

	if cur.remaining() == 0 {
		return nil, fmt.Errorf("pathrep bundle offset %d exceeds data length %d", cur.pos, len(indexData))
	}

	pathrepBundle, err := OpenBundle(bytes.NewReader(indexData[cur.pos:]))
	if err != nil {
		return nil, fmt.Errorf("unable to read pathrep bundle at offset %d: %w", cur.pos, err)
	}

	pathData := make([]byte, pathrepBundle.Size())
	if _, err := pathrepBundle.ReadAt(pathData, 0); err != nil {
		return nil, fmt.Errorf("unable to read pathrep bundle: %w", err)
	}

	for _, pr := range pathmap {
		end := int64(pr.offset) + int64(pr.size)
		if end > int64(len(pathData)) {
			return nil, fmt.Errorf("pathrep span [%d:%d] exceeds path data size %d", pr.offset, end, len(pathData))
		}
		data := pathData[pr.offset:end]
		paths := readPathspec(data)
		for _, path := range paths {
			modernHash := MurmurHashPath(path)
			if fe, found := filemap[modernHash]; found {
				files[fe].path = path
			} else {
				legacyHash := FNVHashPath(path)
				if fe, found := filemap[legacyHash]; found {
					files[fe].path = path
				} else {
					continue
				}
			}
		}
	}

	// Files whose hash matched no pathrep entry have no path; drop them so
	// they cannot pollute listings or match empty-string lookups.
	matched := files[:0]
	unmatched := 0
	for _, f := range files {
		if f.path == "" {
			unmatched++
			continue
		}
		matched = append(matched, f)
	}
	files = matched
	if unmatched > 0 {
		slog.Warn("Dropping index files with no matching path", "count", unmatched)
	}

	// Lookups binary-search with a case-folded predicate, so the sort order
	// must use the same fold or the search invariant breaks on mixed-case
	// (legacy) indices.
	sort.Slice(files, func(i, j int) bool {
		return strings.ToLower(files[i].path) < strings.ToLower(files[j].path)
	})

	return &Index{
		bundles: bundles,
		files:   files,
	}, nil
}

// byteCursor is a bounds-checked reader over the raw index data; every read
// returns an error instead of panicking on truncated or corrupt input.
type byteCursor struct {
	data []byte
	pos  int
}

func (c *byteCursor) remaining() int { return len(c.data) - c.pos }

func (c *byteCursor) take(n int) ([]byte, error) {
	if n < 0 || n > c.remaining() {
		return nil, fmt.Errorf("index truncated: need %d bytes at offset %d, have %d", n, c.pos, c.remaining())
	}
	b := c.data[c.pos : c.pos+n]
	c.pos += n
	return b, nil
}

func (c *byteCursor) uint32() (uint32, error) {
	b, err := c.take(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
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

func readPathspecString(data []byte, offset *int) string {
	p := *offset
	for p < len(data) && data[p] != 0 {
		p++
	}
	s := string(data[*offset:p])
	*offset = p + 1
	return s
}

func (idx *Index) find(path string) *bundleFileInfo {
	files := idx.files
	lowerPath := strings.ToLower(path)

	i := sort.Search(len(files), func(i int) bool {
		return strings.ToLower(files[i].path) >= lowerPath
	})

	if i < len(files) && strings.ToLower(files[i].path) == lowerPath {
		return &files[i]
	}
	return nil
}

func (idx *Index) GetFileInfo(path string) (*FileLocation, error) {
	file := idx.find(path)
	if file == nil {
		return nil, fmt.Errorf("file not found: %s", path)
	}
	return &FileLocation{
		BundleName: idx.bundles[file.bundleId],
		Offset:     file.offset,
		Size:       file.size,
	}, nil
}

func (idx *Index) ListFiles() []string {
	files := make([]string, len(idx.files))
	for i, file := range idx.files {
		files[i] = file.path
	}
	return files
}

func (idx *Index) ListFileEntries() []FileEntry {
	entries := make([]FileEntry, len(idx.files))
	for i, file := range idx.files {
		entries[i] = FileEntry{Path: file.path, Size: file.size}
	}
	return entries
}

func (idx *Index) ListFilesWithPrefix(prefix string) []string {
	files := idx.files
	lowerPrefix := strings.ToLower(prefix)
	if !strings.HasSuffix(lowerPrefix, "/") {
		lowerPrefix += "/"
	}

	start := sort.Search(len(files), func(i int) bool {
		return strings.ToLower(files[i].path) >= lowerPrefix
	})

	var result []string
	for i := start; i < len(files); i++ {
		lower := strings.ToLower(files[i].path)
		if !strings.HasPrefix(lower, lowerPrefix) {
			break
		}
		result = append(result, files[i].path)
	}
	return result
}

func (idx *Index) ExpandFilePaths(paths []string) []string {
	var expanded []string
	for _, p := range paths {
		if idx.find(p) != nil {
			expanded = append(expanded, p)
			continue
		}
		children := idx.ListFilesWithPrefix(p)
		if len(children) > 0 {
			expanded = append(expanded, children...)
		} else {
			expanded = append(expanded, p)
		}
	}
	return expanded
}
