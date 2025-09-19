package bundle

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/jchantrell/exiledb/internal/cache"
)

// BundleManager provides a high-level API for accessing bundle files
type BundleManager struct {
	cacheDir  string
	patch     string
	index     *bundleIndex
	languages []string
	cache     *cache.Cache
}

// NewBundleManager creates a new bundle manager
func NewBundleManager(cacheDir, patch string) (*BundleManager, error) {
	// Load the index from the cache directory using patch version
	indexPath := filepath.Join(cacheDir, patch, "_.index.bin")

	// Check if index file exists
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("index file not found: %s", indexPath)
	}

	// Open and read the index file
	indexFile, err := os.Open(indexPath)
	if err != nil {
		return nil, fmt.Errorf("opening index file: %w", err)
	}
	defer indexFile.Close()

	// Load the bundle index
	index, err := loadBundleIndex(indexFile)
	if err != nil {
		return nil, fmt.Errorf("loading bundle index: %w", err)
	}

	manager := &BundleManager{
		cacheDir:  cacheDir,
		patch:     patch,
		index:     &index,
		languages: []string{"English"}, // Default to English only
		cache:     cache.CacheManager(),
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

		// Get the bundle file content
		content, err := m.readFileFromBundle(fileInfo)
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

// Close closes the manager and releases resources
func (m *BundleManager) Close() error {
	// Nothing to close for now, but keeping the interface for future use
	return nil
}

// findFileInIndex searches for a file in the loaded index
func (m *BundleManager) findFileInIndex(path string) *bundleFileInfo {
	files := m.index.files

	// Binary search for the file
	left, right := 0, len(files)-1
	for left <= right {
		mid := left + (right-left)/2
		if files[mid].path == path {
			return &files[mid]
		}
		if files[mid].path < path {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return nil
}

// readFileFromBundle reads a file's content from its bundle
func (m *BundleManager) readFileFromBundle(fileInfo *bundleFileInfo) ([]byte, error) {
	// Get bundle name
	bundleName := m.index.bundles[fileInfo.bundleId]

	// Use cache manager to get the correct bundle path (with proper name resolution)
	bundlePath := m.cache.GetBundlePath(m.patch, bundleName+".bundle.bin")

	// Check if it's a direct file (legacy format)
	if _, err := os.Stat(bundlePath); os.IsNotExist(err) {
		bundlePath = m.cache.GetBundlePath(m.patch, bundleName)
	}

	// Check if bundle file exists
	if _, err := os.Stat(bundlePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("bundle file does not exist: %s", bundlePath)
	}

	// Check if it's a direct DAT file (legacy format) by examining the file structure
	// Bundle files can have .dat64 extensions but contain bundle headers
	if strings.HasSuffix(bundlePath, ".dat64") || strings.HasSuffix(bundlePath, ".dat") {
		// Check if this is actually a bundle by trying to read its header
		if isDirectDATFile, err := m.isDirectDATFile(bundlePath); err == nil && isDirectDATFile {
			// This is a standalone DAT file, not a bundle container
			fileData, err := os.ReadFile(bundlePath)
			if err != nil {
				return nil, fmt.Errorf("reading direct DAT file %s: %w", bundlePath, err)
			}
			return fileData, nil
		}
		// If error checking or it's not a direct DAT, continue with bundle processing
	}

	// Open bundle file
	bundleFile, err := os.Open(bundlePath)
	if err != nil {
		return nil, fmt.Errorf("opening bundle file %s: %w", bundlePath, err)
	}
	defer bundleFile.Close()

	// Open the bundle using the low-level bundle reader
	bundle, err := OpenBundle(bundleFile)
	if err != nil {
		return nil, fmt.Errorf("opening bundle %s: %w", bundleName, err)
	}

	// Read the specific file data from the bundle
	fileData := make([]byte, fileInfo.size)
	_, err = bundle.ReadAt(fileData, int64(fileInfo.offset))
	if err != nil {
		return nil, fmt.Errorf("reading file data from bundle (offset=%d, size=%d): %w", fileInfo.offset, fileInfo.size, err)
	}

	return fileData, nil
}

// resolvePaths generates all possible paths for a file based on configured languages
func (m *BundleManager) resolvePaths(inputPath string) []string {
	var paths []string

	// Handle both uppercase Data/ and lowercase data/ paths
	var filename string

	if strings.HasPrefix(inputPath, "Data/") {
		filename = strings.TrimPrefix(inputPath, "Data/")
	} else if strings.HasPrefix(inputPath, "data/") {
		filename = strings.TrimPrefix(inputPath, "data/")
	} else {
		// If path doesn't start with data path, return as-is
		return []string{inputPath}
	}

	// Try each configured language with lowercase data/ paths
	for _, lang := range m.languages {
		var langPath string
		if lang == "English" {
			// English files are directly in data/
			langPath = "data/" + filename
		} else {
			// Other languages have subdirectories with lowercase language names
			langLower := strings.ToLower(lang)
			langPath = "data/" + langLower + "/" + filename
		}
		paths = append(paths, langPath)
	}

	return paths
}

// isDirectDATFile determines if a file is a standalone DAT file vs a bundle container
// by examining its structure for the characteristic boundary marker
func (m *BundleManager) isDirectDATFile(filePath string) (bool, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return false, err
	}
	defer file.Close()

	// Read enough data to check for DAT file structure
	// We need at least: 4 bytes (row count) + some fixed data + 8 bytes (boundary marker)
	buffer := make([]byte, 1024)
	n, err := file.Read(buffer)
	if err != nil && err != io.EOF {
		return false, err
	}

	if n < 12 { // Need at least 4 bytes row count + 8 bytes boundary marker
		return false, nil
	}

	// Check for the characteristic DAT boundary marker
	boundaryMarker := []byte{0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb}

	// Look for boundary marker in the first part of the file
	for i := 4; i <= n-8; i++ { // Start from byte 4 (after potential row count)
		if bytes.Equal(buffer[i:i+8], boundaryMarker) {
			// Found boundary marker, this looks like a DAT file
			// Also check that the row count is reasonable
			rowCount := binary.LittleEndian.Uint32(buffer[0:4])
			if rowCount > 0 && rowCount < 1000000 { // Reasonable row count
				return true, nil
			}
		}
	}

	// No boundary marker found in expected location, likely a bundle file
	return false, nil
}
