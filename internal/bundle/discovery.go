package bundle

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/jchantrell/exiledb/internal/cache"
)

type BytesReaderAt struct {
	data []byte
}

var ext = ".datc64"

func DiscoverRequiredBundles(cache *cache.Cache, patch string, languages []string, tables []string) ([]string, error) {
	indexPath := cache.GetIndexPath(patch)

	slog.Info("Reading index file", "path", indexPath)
	indexData, err := os.ReadFile(indexPath)
	if err != nil {
		return nil, fmt.Errorf("reading index file: %w", err)
	}

	decompressedIndexData, err := DecompressIndexBundle(indexData)
	if err != nil {
		return nil, fmt.Errorf("decompressing index bundle: %w", err)
	}

	index, err := LoadIndex(decompressedIndexData)
	if err != nil {
		return nil, fmt.Errorf("parsing index bundle: %w", err)
	}

	bundleSet := GetBundleSet(index, tables, languages)

	var candidatePaths []string
	for bundle := range bundleSet {
		candidatePaths = append(candidatePaths, bundle)
	}

	return candidatePaths, nil
}

func DecompressIndexBundle(data []byte) ([]byte, error) {
	reader := &BytesReaderAt{data: data}

	b, err := OpenBundle(reader)
	if err != nil {
		return nil, fmt.Errorf("opening bundle: %w", err)
	}

	return b.Read()
}

func GetBundleSet(index Index, tables, languages []string) map[string]bool {
	bundleSet := make(map[string]bool)
	bundleSet["_.index.bin"] = true

	allFiles := index.ListFiles()
	datFileCount := 0

	if len(tables) > 0 {
		for _, table := range tables {
			lowerTableName := strings.ToLower(table)
			path := fmt.Sprintf("data/%s%s", lowerTableName, ext)
			if loc, err := index.GetFileInfo(path); err == nil {
				bundleSet[loc.BundleName] = true
			} else {
				slog.Warn("Table file not found", "table", table, "path", path, "error", err)
			}

			for _, language := range languages {
				if language == "English" {
					continue
				}
				langPath := fmt.Sprintf("data/%s/%s%s", strings.ToLower(language), lowerTableName, ext)
				if loc, err := index.GetFileInfo(langPath); err == nil {
					bundleSet[loc.BundleName] = true
				}
			}
		}

	} else {
		for _, filePath := range allFiles {
			if strings.HasPrefix(filePath, "data/") && strings.HasSuffix(filePath, ext) {
				if loc, err := index.GetFileInfo(filePath); err == nil {
					bundleSet[loc.BundleName] = true
					datFileCount++
				}
			}
		}
	}

	return bundleSet
}

func (r *BytesReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off:])
	return n, nil
}
