package cache

import (
	"os"
	"path/filepath"
	"strings"
)

// Cache handles cache directory operations and file validation
type Cache struct{}

// GetCacheManager creates a new cache manager
func CacheManager() *Cache {
	return &Cache{}
}

// GetPatchDir returns the cache directory for a patch version
func (m *Cache) GetCacheDir() string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		// Fallback to current directory
		return filepath.Join(".", ".exiledb", "cache")
	}
	return filepath.Join(homeDir, ".exiledb", "cache")
}

// GetPatchDir returns the cache directory for a patch version
func (m *Cache) GetPatchDir(patch string) string {
	return filepath.Join(m.GetCacheDir(), patch)
}

// EnsureDir creates a directory and all parent directories
func (m *Cache) EnsureDir(dir string) error {
	return os.MkdirAll(dir, 0755)
}

// FileExists checks if a file exists
func (m *Cache) FileExists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}

// GetFileSize returns the size of a file, or 0 if it doesn't exist
func (m *Cache) GetFileSize(filename string) int64 {
	info, err := os.Stat(filename)
	if err != nil {
		return 0
	}
	return info.Size()
}

// GetSchemaPath returns the path to a bundle file for a patch
func (m *Cache) GetSchemaPath() string {
	return filepath.Join(m.GetCacheDir(), "schema.min.json")
}

// GetIndexPath returns the path to the index file for a patch
func (m *Cache) GetIndexPath(patch string) string {
	return filepath.Join(m.GetPatchDir(patch), "_.index.bin")
}

// GetBundlePath returns the path to a bundle file for a patch
func (m *Cache) GetBundlePath(patch, bundleName string) string {
	// Replace forward slashes with underscores to avoid directory conflicts
	// This prevents issues where bundle names like "Folders/metadata" and "Folders/metadata/49/statdescriptions"
	// would create conflicting file and directory paths
	safeBundleName := strings.ReplaceAll(bundleName, "/", "_")

	// Replace spaces with underscores for Windows compatibility
	// This ensures consistent filename handling across all platforms
	safeBundleName = strings.ReplaceAll(safeBundleName, " ", "_")

	return filepath.Join(m.GetPatchDir(patch), safeBundleName)
}

