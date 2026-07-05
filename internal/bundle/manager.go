package bundle

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
)

type BundleManager struct {
	source BundleSource
	index  *Index

	mu          sync.Mutex
	closed      bool
	bundleCache map[string]*cachedBundle
}

type cachedBundle struct {
	bundle *bundle
	closer io.Closer
}

func LoadIndex(source BundleSource) (*Index, error) {
	indexData, err := source.ReadIndex()
	if err != nil {
		return nil, fmt.Errorf("reading index: %w", err)
	}

	index, err := LoadIndexCached(indexData, source.IndexCachePath())
	if err != nil {
		return nil, fmt.Errorf("loading bundle index: %w", err)
	}
	return index, nil
}

func NewBundleManager(source BundleSource) (*BundleManager, error) {
	index, err := LoadIndex(source)
	if err != nil {
		return nil, err
	}

	slog.Debug("Bundle index loaded", "file_count", len(index.files))

	return &BundleManager{
		source:      source,
		index:       index,
		bundleCache: make(map[string]*cachedBundle),
	}, nil
}

func (m *BundleManager) Index() *Index {
	return m.index
}

func (m *BundleManager) FileExists(path string) bool {
	return m.index.find(path) != nil
}

func (m *BundleManager) getBundle(bundleName string) (*bundle, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil, fmt.Errorf("bundle manager is closed")
	}

	if cached, ok := m.bundleCache[bundleName]; ok {
		return cached.bundle, nil
	}

	r, closer, err := m.source.OpenBundle(bundleName)
	if err != nil {
		return nil, err
	}

	b, err := OpenBundle(r)
	if err != nil {
		if closer != nil {
			closer.Close()
		}
		return nil, fmt.Errorf("opening bundle %s: %w", bundleName, err)
	}

	m.bundleCache[bundleName] = &cachedBundle{bundle: b, closer: closer}
	return b, nil
}

func (m *BundleManager) readFileFromBundle(bundleName string, offset, size uint32) ([]byte, error) {
	b, err := m.getBundle(bundleName)
	if err != nil {
		return nil, err
	}

	data := make([]byte, size)
	if _, err := b.ReadAt(data, int64(offset)); err != nil {
		return nil, fmt.Errorf("reading from bundle %s (offset=%d, size=%d): %w", bundleName, offset, size, err)
	}

	return data, nil
}

func (m *BundleManager) GetFile(path string) ([]byte, error) {
	info := m.index.find(path)
	if info == nil {
		return nil, fmt.Errorf("file not found: %s", path)
	}

	bundleName := m.index.bundles[info.bundleId]
	slog.Debug("Found file in index", "bundle_id", info.bundleId, "bundle_name", bundleName, "size", info.size, "offset", info.offset)

	content, err := m.readFileFromBundle(bundleName, info.offset, info.size)
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
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}
	m.closed = true

	var errs []error
	for name, cached := range m.bundleCache {
		if cached.closer == nil {
			continue
		}
		if err := cached.closer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing bundle %s: %w", name, err))
		}
	}
	m.bundleCache = nil

	if err := m.source.Close(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
