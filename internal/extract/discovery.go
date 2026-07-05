package extract

import (
	"log/slog"
	"strings"

	"github.com/jchantrell/exiledb/internal/bundle"
	"github.com/jchantrell/exiledb/internal/export"
	"github.com/jchantrell/exiledb/internal/poe"
)

func discoverRequiredBundles(index *bundle.Index, patch string, languages []string, tables []string, files []string) []string {
	bundleSet := make(map[string]bool)

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
		for _, filePath := range index.ListFiles() {
			if strings.HasPrefix(filePath, "data/") && strings.HasSuffix(filePath, poe.DatExtension) {
				if loc, err := index.GetFileInfo(filePath); err == nil {
					bundleSet[loc.BundleName] = true
				}
			}
		}
	}

	if len(files) > 0 {
		needsSpriteIndices := false

		for _, filePath := range index.ExpandFilePaths(files) {
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
				} else {
					slog.Warn("Sprite index file not found", "path", spriteList.Path, "error", err)
				}
			}
		}
	}

	bundles := make([]string, 0, len(bundleSet))
	for b := range bundleSet {
		bundles = append(bundles, b)
	}
	return bundles
}
