package cdn

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/jchantrell/exiledb/internal/cache"
)

const (
	PoE1CDNURL = "https://patch.poecdn.com"
	PoE2CDNURL = "https://patch-poe2.poecdn.com"

	downloadConcurrency = 6
)

func ConstructURL(gameVersion int, patch string, filename string) string {
	var baseURL string
	if gameVersion >= 4 {
		baseURL = PoE2CDNURL
	} else {
		baseURL = PoE1CDNURL
	}
	return fmt.Sprintf("%s/%s/Bundles2/%s", baseURL, patch, filename)
}

func DownloadIndex(ctx context.Context, cache *cache.Cache, patch string, gameVersion int, force bool) error {
	indexPath := cache.IndexPath(patch)

	// Downloads land atomically (temp file + rename), so existence means a
	// completed download.
	if info, err := os.Stat(indexPath); !force && err == nil && info.Size() > 0 {
		return nil
	}

	indexURL := ConstructURL(gameVersion, patch, "_.index.bin")
	slog.Info("Fetching bundles from CDN", "url", indexURL, "destination", indexPath)

	if err := os.MkdirAll(filepath.Dir(indexPath), 0755); err != nil {
		return fmt.Errorf("creating cache directory: %w", err)
	}

	if err := Download(ctx, indexURL, indexPath); err != nil {
		return fmt.Errorf("downloading index file: %w", err)
	}

	return nil
}

type ProgressFunc func(done, total int, description string)

func DownloadBundles(ctx context.Context, cache *cache.Cache, patch string, gameVersion int, bundleNames []string, force bool, progress ProgressFunc) error {
	var downloadableCount int
	bundlesToDownload := make([]string, 0, len(bundleNames))

	for _, bundleName := range bundleNames {
		bundlePath := cache.BundlePath(patch, bundleName)

		if !force {
			if info, err := os.Stat(bundlePath); err == nil && info.Size() > 0 {
				slog.Debug("Bundle already cached", "bundle", bundleName, "size", info.Size())
				continue
			}
		}

		bundlesToDownload = append(bundlesToDownload, bundleName)
		downloadableCount++
	}

	if downloadableCount == 0 {
		slog.Info("Using cached bundles")
		return nil
	}

	slog.Info("Downloading bundles", "count", downloadableCount)
	sort.Strings(bundlesToDownload)

	var downloaded atomic.Int64
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(downloadConcurrency)
	for _, bundleName := range bundlesToDownload {
		g.Go(func() error {
			bundlePath := cache.BundlePath(patch, bundleName)

			if err := os.MkdirAll(filepath.Dir(bundlePath), 0755); err != nil {
				return fmt.Errorf("creating cache directory for bundle %s: %w", bundleName, err)
			}

			slog.Debug("Downloading bundle", "bundle", bundleName)
			if err := Download(ctx, ConstructURL(gameVersion, patch, bundleName+".bundle.bin"), bundlePath); err != nil {
				return fmt.Errorf("downloading bundle %s: %w", bundleName, err)
			}

			if progress != nil {
				progress(int(downloaded.Add(1)), downloadableCount, bundleName)
			}
			return nil
		})
	}

	return g.Wait()
}
