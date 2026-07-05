// Package cache owns the on-disk layout of downloaded artifacts: the bundle
// index, bundle files, and the community schema, all under one root.
package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Cache struct {
	root string
}

// New fails rather than falling back to a relative path so cached data never
// silently lands in the working directory.
func New() (*Cache, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("resolving home directory for cache: %w", err)
	}
	return &Cache{root: filepath.Join(homeDir, ".exiledb", "cache")}, nil
}

func (c *Cache) Dir() string {
	return c.root
}

func (c *Cache) PatchDir(patch string) string {
	return filepath.Join(c.root, patch)
}

func (c *Cache) SchemaPath() string {
	return filepath.Join(c.root, "schema.min.json")
}

func (c *Cache) IndexPath(patch string) string {
	return filepath.Join(c.PatchDir(patch), "_.index.bin")
}

// BundlePath is the single owner of bundle file locations; the CDN writer and
// the bundle reader both derive paths from it so they can never disagree.
func (c *Cache) BundlePath(patch, bundleName string) string {
	safeBundleName := strings.ReplaceAll(bundleName, "/", "_")
	safeBundleName = strings.ReplaceAll(safeBundleName, " ", "_")
	return filepath.Join(c.PatchDir(patch), safeBundleName)
}
