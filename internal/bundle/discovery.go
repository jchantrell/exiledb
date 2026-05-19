package bundle

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/jchantrell/exiledb/internal/cache"
	"github.com/jchantrell/exiledb/internal/export"
	"github.com/jchantrell/exiledb/internal/utils"
)

type BytesReaderAt struct {
	data []byte
}

var ext = ".datc64"

func DiscoverRequiredBundles(cache *cache.Cache, patch string, languages []string, tables []string, files []string) ([]string, error) {
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

	bundleSet := GetBundleSet(index, patch, tables, languages, files)

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

func GetBundleSet(index Index, patch string, tables, languages []string, files []string) map[string]bool {
	bundleSet := make(map[string]bool)
	bundleSet["_.index.bin"] = true

	allFiles := index.ListFiles()
	datFileCount := 0

	if len(tables) > 0 {
		for _, table := range tables {
			path := utils.DatPath(patch, table, ext)
			if loc, err := index.GetFileInfo(path); err == nil {
				bundleSet[loc.BundleName] = true
			} else {
				slog.Warn("Table file not found", "table", table, "path", path, "error", err)
			}

			for _, language := range languages {
				if language == "English" {
					continue
				}
				langPath := utils.DatLangPath(patch, language, table, ext)
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

	// Add bundles for requested files (expanding directory prefixes)
	if len(files) > 0 {
		expandedFiles := expandFilePaths(index, files)
		needsSpriteIndices := false

		for _, filePath := range expandedFiles {
			if loc, err := index.GetFileInfo(filePath); err == nil {
				bundleSet[loc.BundleName] = true
			} else {
				slog.Warn("File not found in index", "file", filePath, "error", err)
			}

			if export.IsInsideSprite(filePath) {
				needsSpriteIndices = true
			}
		}

		if needsSpriteIndices {
			for _, spriteList := range export.SpriteLists {
				if loc, err := index.GetFileInfo(spriteList.Path); err == nil {
					bundleSet[loc.BundleName] = true
					slog.Debug("Adding sprite index file", "path", spriteList.Path, "bundle", loc.BundleName)
				} else {
					slog.Warn("Sprite index file not found", "path", spriteList.Path, "error", err)
				}
			}
		}
	}

	return bundleSet
}

func expandFilePaths(index Index, paths []string) []string {
	var expanded []string
	for _, p := range paths {
		if _, err := index.GetFileInfo(p); err == nil {
			expanded = append(expanded, p)
			continue
		}
		children := index.ListFilesWithPrefix(p)
		if len(children) > 0 {
			expanded = append(expanded, children...)
		} else {
			expanded = append(expanded, p)
		}
	}
	return expanded
}

func (r *BytesReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off:])
	return n, nil
}
