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

func (c *Cache) BundlePath(patch, bundleName string) string {
	safeBundleName := strings.ReplaceAll(bundleName, "/", "_")
	safeBundleName = strings.ReplaceAll(safeBundleName, " ", "_")
	return filepath.Join(c.PatchDir(patch), safeBundleName)
}
