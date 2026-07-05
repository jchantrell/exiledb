package bundle

import (
	"cmp"
	"fmt"
	"log/slog"
	"slices"
)

type BundleManager struct {
	source BundleSource
	index  *Index
}

func NewBundleManager(source BundleSource) (*BundleManager, error) {
	indexData, err := source.ReadIndex()
	if err != nil {
		return nil, fmt.Errorf("reading index: %w", err)
	}

	index, err := LoadIndexCached(indexData, source.IndexCachePath())
	if err != nil {
		return nil, fmt.Errorf("loading bundle index: %w", err)
	}

	slog.Debug("Bundle index loaded", "file_count", len(index.files))

	return &BundleManager{
		source: source,
		index:  index,
	}, nil
}

func (m *BundleManager) Index() *Index {
	return m.index
}

func (m *BundleManager) FileExists(path string) bool {
	return m.index.find(path) != nil
}

func (m *BundleManager) GetFile(path string) ([]byte, error) {
	info := m.index.find(path)
	if info == nil {
		return nil, fmt.Errorf("file not found: %s", path)
	}

	bundleName := m.index.bundles[info.bundleId]
	slog.Debug("Found file in index", "bundle_id", info.bundleId, "bundle_name", bundleName, "size", info.size, "offset", info.offset)

	content, err := m.source.ReadFileFromBundle(bundleName, info.offset, info.size)
	if err != nil {
		return nil, fmt.Errorf("reading file from bundle: %w", err)
	}

	return content, nil
}

func (m *BundleManager) ExpandFilePaths(paths []string) []string {
	return m.index.ExpandFilePaths(paths)
}

func (m *BundleManager) SortByBundle(paths []string) []string {
	type fileWithBundle struct {
		path     string
		bundleId uint32
	}

	items := make([]fileWithBundle, 0, len(paths))
	for _, p := range paths {
		info := m.index.find(p)
		if info != nil {
			items = append(items, fileWithBundle{path: p, bundleId: info.bundleId})
		} else {
			items = append(items, fileWithBundle{path: p, bundleId: ^uint32(0)})
		}
	}

	slices.SortStableFunc(items, func(a, b fileWithBundle) int {
		return cmp.Compare(a.bundleId, b.bundleId)
	})

	sorted := make([]string, len(items))
	for i, item := range items {
		sorted[i] = item.path
	}
	return sorted
}

func (m *BundleManager) Close() error {
	return m.source.Close()
}
