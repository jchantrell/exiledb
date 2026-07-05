package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jchantrell/exiledb/internal/bundle"
	"github.com/jchantrell/exiledb/internal/cache"
	"github.com/jchantrell/exiledb/internal/cdn"
	"github.com/jchantrell/exiledb/internal/poe"
)

// loadBundleIndex loads the bundle index from a GGPK file if configured,
// otherwise downloads it from the CDN for the configured patch.
func loadBundleIndex(ctx context.Context) (*bundle.Index, error) {
	var indexData []byte
	var cachePath string

	if cfg.GgpkPath != "" {
		source, err := bundle.NewGgpkSource(cfg.GgpkPath)
		if err != nil {
			return nil, fmt.Errorf("opening GGPK: %w", err)
		}
		defer source.Close()

		indexData, err = source.ReadIndex()
		if err != nil {
			return nil, fmt.Errorf("reading index from GGPK: %w", err)
		}
		cachePath = source.IndexCachePath()
	} else {
		c := cache.CacheManager()

		gameVersion, err := poe.ParseGameVersion(cfg.Patch)
		if err != nil {
			return nil, fmt.Errorf("parsing game version: %w", err)
		}

		if err := cdn.DownloadIndex(ctx, c, cfg.Patch, gameVersion, false); err != nil {
			return nil, fmt.Errorf("downloading index file: %w", err)
		}

		indexData, err = os.ReadFile(c.GetIndexPath(cfg.Patch))
		if err != nil {
			return nil, fmt.Errorf("reading index file: %w", err)
		}
		cachePath = c.GetIndexPath(cfg.Patch) + ".cache"
	}

	index, err := bundle.LoadIndexCached(indexData, cachePath)
	if err != nil {
		return nil, fmt.Errorf("loading index: %w", err)
	}

	return index, nil
}
