// Package cdn provides functionality for downloading Path of Exile game bundles from CDN servers.
// It handles URL construction, index file downloads, and bundle downloads with progress tracking
// for both single and multiple bundle operations.
package cdn

import (
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/jchantrell/exiledb/internal/cache"
	"github.com/jchantrell/exiledb/internal/utils"
)

const (
	PoE1CDNURL = "https://patch.poecdn.com"
	PoE2CDNURL = "https://patch-poe2.poecdn.com"
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

func DownloadIndex(cache *cache.Cache, patch string, gameVersion int, force bool) error {
	indexPath := cache.GetIndexPath(patch)

	if !force {
		if cache.FileExists(indexPath) {
			size := cache.GetFileSize(indexPath)
			if size > 0 {
				return nil
			}
		}
	}

	indexURL := ConstructURL(gameVersion, patch, "_.index.bin")
	slog.Info("Fetching bundles from CDN", "url", indexURL, "destination", indexPath)

	if err := cache.EnsureDir(filepath.Dir(indexPath)); err != nil {
		return fmt.Errorf("creating cache directory: %w", err)
	}

	if err := utils.DownloadFile(indexPath, indexURL); err != nil {
		return fmt.Errorf("downloading index file from %s: %w", indexURL, err)
	}

	if !cache.FileExists(indexPath) {
		return fmt.Errorf("downloaded index file is missing")
	}

	size := cache.GetFileSize(indexPath)
	if size == 0 {
		return fmt.Errorf("downloaded index file is empty")
	}

	return nil
}

func DownloadBundles(cache *cache.Cache, patch string, gameVersion int, bundleNames []string, force bool, progressEnabled bool) error {
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

	bundleProgress := utils.NewProgress(downloadableCount, progressEnabled)

	currentProgress := 0
	for _, bundleName := range bundlesToDownload {
		bundlePath := cache.GetBundlePath(patch, bundleName)

		if err := cache.EnsureDir(filepath.Dir(bundlePath)); err != nil {
			return fmt.Errorf("creating cache directory for bundle %s: %w", bundleName, err)
		}

		var cdnFileName string
		if bundleName == "_.index.bin" {
			cdnFileName = bundleName // Keep as _.index.bin
		} else {
			cdnFileName = bundleName + ".bundle.bin"
		}
		bundleURL := ConstructURL(gameVersion, patch, cdnFileName)

		if !progressEnabled {
			slog.Info("Downloading bundle", "bundle", bundleName)
		}
		if err := utils.DownloadFile(bundlePath, bundleURL); err != nil {
			return fmt.Errorf("downloading bundle %s from %s: %w", bundleName, bundleURL, err)
		}

		if !cache.FileExists(bundlePath) {
			return fmt.Errorf("downloaded bundle %s is missing", bundleName)
		}

		size := cache.GetFileSize(bundlePath)
		if size == 0 {
			return fmt.Errorf("downloaded bundle %s is empty", bundleName)
		}

		currentProgress++
		bundleProgress.Update(currentProgress, bundleName)
	}

	bundleProgress.Finish()
	return nil
}
