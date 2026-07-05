package bundle

import (
	"log/slog"
	"strings"

	"github.com/jchantrell/exiledb/internal/export"
	"github.com/jchantrell/exiledb/internal/poe"
)

// DiscoverRequiredBundles determines which bundles must be downloaded to
// satisfy the requested tables, languages, and files.
func DiscoverRequiredBundles(index *Index, patch string, languages []string, tables []string, files []string) []string {
	bundleSet := getBundleSet(index, patch, tables, languages, files)

	var candidatePaths []string
	for bundle := range bundleSet {
		candidatePaths = append(candidatePaths, bundle)
	}

	return candidatePaths
}

func getBundleSet(index *Index, patch string, tables, languages []string, files []string) map[string]bool {
	bundleSet := make(map[string]bool)
	bundleSet["_.index.bin"] = true

	allFiles := index.ListFiles()
	datFileCount := 0

	if len(tables) > 0 {
		for _, table := range tables {
			path := poe.DatPath(patch, table, poe.DatExtension)
			if loc, err := index.GetFileInfo(path); err == nil {
				bundleSet[loc.BundleName] = true
			} else {
				slog.Warn("Table file not found", "table", table, "path", path, "error", err)
			}

			for _, language := range languages {
				if language == "English" {
					continue
				}
				langPath := poe.DatLangPath(patch, language, table, poe.DatExtension)
				if loc, err := index.GetFileInfo(langPath); err == nil {
					bundleSet[loc.BundleName] = true
				}
			}
		}

	} else {
		for _, filePath := range allFiles {
			if strings.HasPrefix(filePath, "data/") && strings.HasSuffix(filePath, poe.DatExtension) {
				if loc, err := index.GetFileInfo(filePath); err == nil {
					bundleSet[loc.BundleName] = true
					datFileCount++
				}
			}
		}
	}

	// Add bundles for requested files (expanding directory prefixes)
	if len(files) > 0 {
		expandedFiles := index.ExpandFilePaths(files)
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
