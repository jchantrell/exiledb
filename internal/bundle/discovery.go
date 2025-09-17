package bundle

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/jchantrell/exiledb/internal/cache"
)

// DiscoverRequiredBundles parses the index file to find which bundles contain the DAT files needed
// based on the specified configuration (languages, tables, allTables flag).
func DiscoverRequiredBundles(cache *cache.Cache, patch string, languages []string, tables []string, allTables bool) ([]string, error) {
	indexPath := cache.GetIndexPath(patch)

	// Read the index file
	slog.Info("Reading index file", "path", indexPath)
	indexData, err := os.ReadFile(indexPath)
	if err != nil {
		return nil, fmt.Errorf("reading index file: %w", err)
	}

	// First, decompress the index bundle using the same logic as regular bundles
	// The _.index.bin file is a compressed bundle that needs to be decompressed first
	decompressedIndexData, err := DecompressIndexBundle(indexData)
	if err != nil {
		return nil, fmt.Errorf("decompressing index bundle: %w", err)
	}

	// Parse the decompressed index bundle
	index, err := LoadIndex(decompressedIndexData)
	if err != nil {
		return nil, fmt.Errorf("parsing index bundle: %w", err)
	}

	// Build list of required bundles based on configuration
	bundleSet := make(map[string]bool)

	// Always include the index bundle
	bundleSet["_.index.bin"] = true

	if allTables {

		// Get all files from the index and find those in the data/ directory with .datc64 extension
		// This approach mirrors the specific tables mode but discovers all DAT files automatically
		allFiles := index.ListFiles()
		datFileCount := 0
		for _, filePath := range allFiles {
			// Look for .datc64 files in the data/ directory
			// Note: paths in index are already lowercase since PoE version 3.21.2+
			if strings.HasPrefix(filePath, "data/") && strings.HasSuffix(filePath, ".datc64") {
				if loc, err := index.GetFileInfo(filePath); err == nil {
					bundleSet[loc.BundleName] = true
					datFileCount++
					slog.Debug("Found DAT file", "path", filePath, "bundle", loc.BundleName)
				}
			}
		}
		slog.Debug("Comprehensive bundle discovery complete", "dat_files_found", datFileCount, "bundles_found", len(bundleSet)-1)
	} else if len(tables) > 0 {
		// Specific tables mode: download only bundles containing requested tables
		slog.Debug("Specific tables mode - downloading bundles for requested tables only", "tables", tables)
		for _, table := range tables {
			// Check for .datc64 files with lowercase data/ and table name
			lowerTableName := strings.ToLower(table)
			for _, ext := range []string{".datc64"} {
				path := fmt.Sprintf("data/%s%s", lowerTableName, ext)
				slog.Debug("Looking for table file", "path", path, "table", table)
				if loc, err := index.GetFileInfo(path); err == nil {
					bundleSet[loc.BundleName] = true
					slog.Debug("Found DAT file for table", "table", table, "path", path, "bundle", loc.BundleName)
				} else {
					slog.Warn("Table file not found", "table", table, "path", path, "error", err)
				}
			}
		}
		slog.Debug("Specific table bundle discovery complete", "requested_tables", len(tables), "bundles_found", len(bundleSet)-1)
	} else {
		// Fallback mode: no specific tables requested, get all bundles containing DAT files
		slog.Debug("No specific tables requested - downloading all bundles containing DAT files")

		// Same logic as AllTables mode - find all DAT files and their bundles
		allFiles := index.ListFiles()
		datFileCount := 0
		for _, filePath := range allFiles {
			// Look for .datc64 files in the data/ directory
			// Note: paths in index are already lowercase since PoE version 3.21.2+
			if strings.HasPrefix(filePath, "data/") && strings.HasSuffix(filePath, ".datc64") {
				if loc, err := index.GetFileInfo(filePath); err == nil {
					bundleSet[loc.BundleName] = true
					datFileCount++
					slog.Debug("Found DAT file", "path", filePath, "bundle", loc.BundleName)
				}
			}
		}
		slog.Debug("Fallback bundle discovery complete", "dat_files_found", datFileCount, "bundles_found", len(bundleSet)-1)
	}

	// Convert set to slice
	var candidatePaths []string
	for bundle := range bundleSet {
		candidatePaths = append(candidatePaths, bundle)
	}

	slog.Debug("Required bundles", "count", len(candidatePaths))
	for i, bundle := range candidatePaths {
		slog.Debug("Bundle to download", "index", i, "name", bundle)
	}

	// candidatePaths already contains the bundle names we need
	return candidatePaths, nil
}

// DecompressIndexBundle decompresses the index bundle using the bundle system
func DecompressIndexBundle(data []byte) ([]byte, error) {
	// Create a reader for the compressed data
	reader := &bytesReaderAt{data: data}

	// Open as bundle
	b, err := OpenBundle(reader)
	if err != nil {
		return nil, fmt.Errorf("opening bundle: %w", err)
	}

	// Read entire contents
	return b.Read()
}

// bytesReaderAt implements io.ReaderAt for a byte slice
type bytesReaderAt struct {
	data []byte
}

func (r *bytesReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off:])
	return n, nil
}
