// Package cdn provides functionality for downloading Path of Exile game bundles from CDN servers.
// It handles URL construction, index file downloads, and bundle downloads with progress tracking
// for both single and multiple bundle operations.
package cdn

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/jchantrell/exiledb/internal/cache"
)

const (
	PoE1CDNURL = "https://patch.poecdn.com"
	PoE2CDNURL = "https://patch-poe2.poecdn.com"

	// downloadConcurrency bounds parallel bundle downloads.
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
	indexPath := cache.GetIndexPath(patch)

	// Downloads land atomically (temp file + rename), so existence means a
	// completed download.
	if !force && cache.FileExists(indexPath) && cache.GetFileSize(indexPath) > 0 {
		return nil
	}

	indexURL := ConstructURL(gameVersion, patch, "_.index.bin")
	slog.Info("Fetching bundles from CDN", "url", indexURL, "destination", indexPath)

	if err := cache.EnsureDir(filepath.Dir(indexPath)); err != nil {
		return fmt.Errorf("creating cache directory: %w", err)
	}

	if err := Download(ctx, indexURL, indexPath); err != nil {
		return fmt.Errorf("downloading index file: %w", err)
	}

	return nil
}

// ProgressFunc reports download progress: done of total, plus the bundle
// currently being downloaded. A nil ProgressFunc disables reporting.
type ProgressFunc func(done, total int, description string)

func DownloadBundles(ctx context.Context, cache *cache.Cache, patch string, gameVersion int, bundleNames []string, force bool, progress ProgressFunc) error {
	var downloadableCount int
	bundlesToDownload := make([]string, 0, len(bundleNames))

	for _, bundleName := range bundleNames {
		bundlePath := cache.GetBundlePath(patch, bundleName)

		if !force {
			if cache.FileExists(bundlePath) {
				size := cache.GetFileSize(bundlePath)
				if size > 0 {
					slog.Debug("Bundle already cached", "bundle", bundleName, "size", size)
					continue
				}
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
			bundlePath := cache.GetBundlePath(patch, bundleName)

			if err := cache.EnsureDir(filepath.Dir(bundlePath)); err != nil {
				return fmt.Errorf("creating cache directory for bundle %s: %w", bundleName, err)
			}

			cdnFileName := bundleName + ".bundle.bin"
			if bundleName == "_.index.bin" {
				cdnFileName = bundleName
			}

			slog.Debug("Downloading bundle", "bundle", bundleName)
			if err := Download(ctx, ConstructURL(gameVersion, patch, cdnFileName), bundlePath); err != nil {
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
