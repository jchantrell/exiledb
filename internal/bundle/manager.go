package bundle

import (
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strings"
)

// BundleManager provides a high-level API for accessing files within bundles.
type BundleManager struct {
	source    BundleSource
	index     *bundleIndex
	languages []string
}

// NewBundleManager creates a bundle manager backed by the given source.
func NewBundleManager(source BundleSource) (*BundleManager, error) {
	indexData, err := source.ReadIndex()
	if err != nil {
		return nil, fmt.Errorf("reading index: %w", err)
	}

	index, err := loadBundleIndexCached(indexData, source.IndexCachePath())
	if err != nil {
		return nil, fmt.Errorf("loading bundle index: %w", err)
	}

	manager := &BundleManager{
		source:    source,
		index:     &index,
		languages: []string{"English"},
	}

	slog.Debug("Bundle index loaded", "file_count", len(index.files))

	return manager, nil
}

// SetLanguages configures the languages to search for files
func (m *BundleManager) SetLanguages(languages []string) {
	if len(languages) == 0 {
		m.languages = []string{"English"}
		return
	}
	m.languages = make([]string, len(languages))
	copy(m.languages, languages)
}

// FileExists checks if a file exists in the bundle, trying language-specific paths as needed
func (m *BundleManager) FileExists(path string) bool {
	paths := m.resolvePaths(path)
	for _, p := range paths {
		if m.findFileInIndex(p) != nil {
			return true
		}
	}
	return false
}

// GetFile reads the entire contents of a file from the bundle, trying language-specific paths as needed
func (m *BundleManager) GetFile(path string) ([]byte, error) {
	paths := m.resolvePaths(path)

	var lastErr error
	for _, p := range paths {
		fileInfo := m.findFileInIndex(p)
		if fileInfo == nil {
			lastErr = fmt.Errorf("file not found: %s", p)
			slog.Debug("File not found in index", "path", p)
			continue
		}

		bundleName := m.index.bundles[fileInfo.bundleId]
		slog.Debug("Found file in index", "bundle_id", fileInfo.bundleId, "bundle_name", bundleName, "size", fileInfo.size, "offset", fileInfo.offset)

		content, err := m.source.ReadFileFromBundle(bundleName, fileInfo.offset, fileInfo.size)
		if err != nil {
			lastErr = fmt.Errorf("reading file from bundle: %w", err)
			slog.Error("Failed to read file from bundle", "path", p, "bundle_name", bundleName, "error", err)
			continue
		}

		return content, nil
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, &fs.PathError{
		Op:   "open",
		Path: path,
		Err:  fs.ErrNotExist,
	}
}

// ExpandFilePaths expands a list of paths, replacing directory prefixes with all files under them.
// Exact file matches are kept as-is; paths that match a directory prefix are expanded recursively.
func (m *BundleManager) ExpandFilePaths(paths []string) []string {
	var expanded []string
	for _, p := range paths {
		if m.findFileInIndex(p) != nil {
			expanded = append(expanded, p)
			continue
		}
		children := m.listFilesWithPrefix(p)
		if len(children) > 0 {
			expanded = append(expanded, children...)
		} else {
			expanded = append(expanded, p)
		}
	}
	return expanded
}

// Close closes the manager and releases resources
func (m *BundleManager) Close() error {
	return m.source.Close()
}

// listFilesWithPrefix returns all files whose path starts with the given prefix (case-insensitive)
func (m *BundleManager) listFilesWithPrefix(prefix string) []string {
	files := m.index.files
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

// findFileInIndex searches for a file in the loaded index (case-insensitive)
func (m *BundleManager) findFileInIndex(path string) *bundleFileInfo {
	files := m.index.files
	lowerPath := strings.ToLower(path)

	left, right := 0, len(files)-1
	for left <= right {
		mid := left + (right-left)/2
		midPathLower := strings.ToLower(files[mid].path)
		if midPathLower == lowerPath {
			return &files[mid]
		}
		if midPathLower < lowerPath {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return nil
}

// resolvePaths generates all possible paths for a file based on configured languages
func (m *BundleManager) resolvePaths(inputPath string) []string {
	var paths []string

	var filename string

	if strings.HasPrefix(inputPath, "Data/") {
		filename = strings.TrimPrefix(inputPath, "Data/")
	} else if strings.HasPrefix(inputPath, "data/") {
		filename = strings.TrimPrefix(inputPath, "data/")
	} else {
		return []string{inputPath}
	}

	for _, lang := range m.languages {
		var langPath string
		if lang == "English" {
			langPath = "data/" + filename
		} else {
			langLower := strings.ToLower(lang)
			langPath = "data/" + langLower + "/" + filename
		}
		paths = append(paths, langPath)
	}

	return paths
}
