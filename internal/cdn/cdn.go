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

// Constants for CDN operations
const (
	// CDN base URLs for different game versions
	PoE1CDNURL = "https://patch.poecdn.com"
	PoE2CDNURL = "https://patch-poe2.poecdn.com"

	// Default download configuration
	DefaultRetries = 3
	DefaultTimeout = 30 // seconds
)

// ConstructURL builds the CDN URL based on game version and patch.
// Game versions >= 4 use the PoE2 CDN, while older versions use PoE1 CDN.
func ConstructURL(gameVersion int, patch string, filename string) string {
	var baseURL string
	if gameVersion >= 4 {
		baseURL = PoE2CDNURL
	} else {
		baseURL = PoE1CDNURL
	}
	return fmt.Sprintf("%s/%s/Bundles2/%s", baseURL, patch, filename)
}

// DownloadIndex downloads the _.index.bin file from CDN with progress tracking.
// The index file contains metadata about all available bundles for a patch version.
func DownloadIndex(cache *cache.Cache, patch string, gameVersion int, force bool) error {
	indexPath := cache.GetIndexPath(patch)

	// Check if index file already exists and is not forced to re-download
	if !force {
		if cache.FileExists(indexPath) {
			size := cache.GetFileSize(indexPath)
			if size > 0 {
				return nil
			}
		}
	}

	// Construct CDN URL for index file
	indexURL := ConstructURL(gameVersion, patch, "_.index.bin")
	slog.Info("Fetching bundles from CDN", "url", indexURL, "destination", indexPath)

	// Create cache directory if it doesn't exist
	if err := cache.EnsureDir(filepath.Dir(indexPath)); err != nil {
		return fmt.Errorf("creating cache directory: %w", err)
	}

	// Download the file
	if err := utils.DownloadFile(indexPath, indexURL); err != nil {
		return fmt.Errorf("downloading index file from %s: %w", indexURL, err)
	}

	// Verify the downloaded file
	if !cache.FileExists(indexPath) {
		return fmt.Errorf("downloaded index file is missing")
	}

	size := cache.GetFileSize(indexPath)
	if size == 0 {
		return fmt.Errorf("downloaded index file is empty")
	}

	return nil
}

// DownloadBundles downloads a list of bundles from the CDN.
func DownloadBundles(cache *cache.Cache, patch string, gameVersion int, bundleNames []string, force bool) error {
	// Check which bundles need downloading
	var downloadableCount int
	bundlesToDownload := make([]string, 0, len(bundleNames))

	for _, bundleName := range bundleNames {
		bundlePath := cache.GetBundlePath(patch, bundleName)

		// Check if bundle already exists and is not forced to re-download
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

	// Download bundles
	for _, bundleName := range bundlesToDownload {
		bundlePath := cache.GetBundlePath(patch, bundleName)

		// Create cache directory if it doesn't exist
		if err := cache.EnsureDir(filepath.Dir(bundlePath)); err != nil {
			return fmt.Errorf("creating cache directory for bundle %s: %w", bundleName, err)
		}

		// Construct CDN URL for bundle
		var cdnFileName string
		if bundleName == "_.index.bin" {
			cdnFileName = bundleName // Keep as _.index.bin
		} else {
			cdnFileName = bundleName + ".bundle.bin"
		}
		bundleURL := ConstructURL(gameVersion, patch, cdnFileName)

		slog.Info("Downloading bundle", "bundle", bundleName)
		if err := utils.DownloadFile(bundlePath, bundleURL); err != nil {
			return fmt.Errorf("downloading bundle %s from %s: %w", bundleName, bundleURL, err)
		}

		// Verify the downloaded file
		if !cache.FileExists(bundlePath) {
			return fmt.Errorf("downloaded bundle %s is missing", bundleName)
		}

		size := cache.GetFileSize(bundlePath)
		if size == 0 {
			return fmt.Errorf("downloaded bundle %s is empty", bundleName)
		}
	}

	return nil
}

// DownloadBundle downloads a single bundle.
// This function handles the download of both regular bundles and the special index bundle.
func DownloadBundle(cache *cache.Cache, patch string, gameVersion int, bundleName string, force bool) error {
	bundlePath := cache.GetBundlePath(patch, bundleName)

	// Check if bundle already exists and is not forced to re-download
	if !force {
		if cache.FileExists(bundlePath) {
			size := cache.GetFileSize(bundlePath)
			if size > 0 {
				slog.Debug("Bundle already cached", "bundle", bundleName, "size", size)
				return nil
			}
		}
	}

	// Construct CDN URL for bundle
	var cdnFileName string
	if bundleName == "_.index.bin" {
		cdnFileName = bundleName // Keep as _.index.bin
	} else {
		cdnFileName = bundleName + ".bundle.bin"
	}
	bundleURL := ConstructURL(gameVersion, patch, cdnFileName)

	// Create cache directory if it doesn't exist
	if err := cache.EnsureDir(filepath.Dir(bundlePath)); err != nil {
		return fmt.Errorf("creating cache directory for bundle %s: %w", bundleName, err)
	}

	// Download the bundle
	slog.Info("Downloading bundle", "bundle", bundleName)
	if err := utils.DownloadFile(bundlePath, bundleURL); err != nil {
		return fmt.Errorf("downloading bundle %s from %s: %w", bundleName, bundleURL, err)
	}

	// Verify the downloaded file
	if !cache.FileExists(bundlePath) {
		return fmt.Errorf("downloaded bundle %s is missing", bundleName)
	}

	size := cache.GetFileSize(bundlePath)
	if size == 0 {
		return fmt.Errorf("downloaded bundle %s is empty", bundleName)
	}

	return nil
}
