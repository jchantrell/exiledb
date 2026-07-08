package extract

import (
	"log/slog"

	"github.com/jchantrell/exiledb/internal/bundle"
	"github.com/jchantrell/exiledb/internal/dat"
	"github.com/jchantrell/exiledb/internal/export"
	"github.com/jchantrell/exiledb/internal/poe"
)

// datFilePaths derives the dat file path for every requested (table, language)
// pair straight from the resolved schema names, so discovery and insertion
// agree on exactly which files to fetch.
func datFilePaths(patch string, tables []dat.TableSchema, languages []string) []string {
	paths := make([]string, 0, len(tables)*len(languages)*len(poe.DatExtensions))
	for _, table := range tables {
		for _, language := range languages {
			for _, ext := range poe.DatExtensions {
				if language == "English" {
					paths = append(paths, poe.DatPath(patch, table.Name, ext))
				} else {
					paths = append(paths, poe.DatLangPath(patch, language, table.Name, ext))
				}
			}
		}
	}
	return paths
}

// resolveDatPath returns the actual dat file path for a table/language by
// trying each 64-bit extension in preference order and returning the first
// that exists — patches before ~2023 use .dat64, current ones .datc64.
func resolveDatPath(patch, table, language string, exists func(string) bool) (string, bool) {
	for _, ext := range poe.DatExtensions {
		p := poe.DatPath(patch, table, ext)
		if language != "English" {
			p = poe.DatLangPath(patch, language, table, ext)
		}
		if exists(p) {
			return p, true
		}
	}
	return "", false
}

// bundlesForFiles resolves each path to the bundle that stores it. Paths that
// live inside a sprite pull in the sprite index files as well, since those
// must be present before sprite sheets can be resolved.
func bundlesForFiles(index *bundle.Index, paths []string) []string {
	bundleSet := make(map[string]bool)
	needsSpriteIndices := false

	for _, path := range paths {
		if loc, err := index.GetFileInfo(path); err == nil {
			bundleSet[loc.BundleName] = true
		} else {
			slog.Debug("File not found in index", "file", path, "error", err)
		}
		if export.IsInsideSprite(path) {
			needsSpriteIndices = true
		}
	}

	if needsSpriteIndices {
		for _, spriteList := range export.SpriteLists {
			if loc, err := index.GetFileInfo(spriteList.Path); err == nil {
				bundleSet[loc.BundleName] = true
			} else {
				slog.Debug("Sprite index file not found", "path", spriteList.Path, "error", err)
			}
		}
	}

	bundles := make([]string, 0, len(bundleSet))
	for b := range bundleSet {
		bundles = append(bundles, b)
	}
	return bundles
}
