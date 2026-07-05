package bundle

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/jchantrell/exiledb/internal/cache"
	"github.com/jchantrell/exiledb/internal/ggpk"
)

// BundleSource abstracts where bundle data comes from (disk cache vs GGPK archive).
type BundleSource interface {
	ReadIndex() ([]byte, error)
	ReadFileFromBundle(bundleName string, offset, size uint32) ([]byte, error)
	IndexCachePath() string
	Close() error
}

// CacheSource reads bundles from the local disk cache (CDN-downloaded files).
type CacheSource struct {
	patch       string
	cache       *cache.Cache
	mu          sync.Mutex
	bundleCache map[string]*cachedBundle
}

type cachedBundle struct {
	bundle *bundle
	file   *os.File
}

func NewCacheSource(c *cache.Cache, patch string) *CacheSource {
	return &CacheSource{
		patch:       patch,
		cache:       c,
		bundleCache: make(map[string]*cachedBundle),
	}
}

func (s *CacheSource) ReadIndex() ([]byte, error) {
	return os.ReadFile(s.cache.IndexPath(s.patch))
}

func (s *CacheSource) getBundle(bundleName string) (*bundle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if cached, ok := s.bundleCache[bundleName]; ok {
		return cached.bundle, nil
	}

	bundlePath := s.cache.BundlePath(s.patch, bundleName+".bundle.bin")

	if _, err := os.Stat(bundlePath); os.IsNotExist(err) {
		bundlePath = s.cache.BundlePath(s.patch, bundleName)
	}

	if _, err := os.Stat(bundlePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("bundle file does not exist: %s", bundlePath)
	}

	if strings.HasSuffix(bundlePath, ".dat64") || strings.HasSuffix(bundlePath, ".dat") {
		if isDirect, err := isDirectDATFile(bundlePath); err == nil && isDirect {
			return nil, nil
		}
	}

	bundleFile, err := os.Open(bundlePath)
	if err != nil {
		return nil, fmt.Errorf("opening bundle file %s: %w", bundlePath, err)
	}

	b, err := OpenBundle(bundleFile)
	if err != nil {
		bundleFile.Close()
		return nil, fmt.Errorf("opening bundle %s: %w", bundleName, err)
	}

	s.bundleCache[bundleName] = &cachedBundle{bundle: b, file: bundleFile}
	return b, nil
}

func (s *CacheSource) ReadFileFromBundle(bundleName string, offset, size uint32) ([]byte, error) {
	b, err := s.getBundle(bundleName)
	if err != nil {
		return nil, err
	}

	if b == nil {
		bundlePath := s.cache.BundlePath(s.patch, bundleName+".bundle.bin")
		if _, err := os.Stat(bundlePath); os.IsNotExist(err) {
			bundlePath = s.cache.BundlePath(s.patch, bundleName)
		}
		return os.ReadFile(bundlePath)
	}

	data := make([]byte, size)
	if _, err := b.ReadAt(data, int64(offset)); err != nil {
		return nil, fmt.Errorf("reading from bundle (offset=%d, size=%d): %w", offset, size, err)
	}

	return data, nil
}

func (s *CacheSource) IndexCachePath() string {
	return s.cache.IndexPath(s.patch) + ".cache"
}

func (s *CacheSource) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var errs []error
	for name, cached := range s.bundleCache {
		if err := cached.file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing bundle %s: %w", name, err))
		}
	}
	s.bundleCache = nil
	return errors.Join(errs...)
}

// GgpkSource reads bundles from within a GGPK archive file.
type GgpkSource struct {
	reader      *ggpk.Reader
	ggpkPath    string
	mu          sync.Mutex
	bundleCache map[string]*bundle
}

func NewGgpkSource(ggpkPath string) (*GgpkSource, error) {
	r, err := ggpk.Open(ggpkPath)
	if err != nil {
		return nil, fmt.Errorf("opening GGPK file: %w", err)
	}
	return &GgpkSource{reader: r, ggpkPath: ggpkPath, bundleCache: make(map[string]*bundle)}, nil
}

func (s *GgpkSource) IndexCachePath() string {
	return s.ggpkPath + ".idx"
}

func (s *GgpkSource) ReadIndex() ([]byte, error) {
	rec, err := s.reader.FindFile("Bundles2/_.index.bin")
	if err != nil {
		return nil, fmt.Errorf("finding index in GGPK: %w", err)
	}
	return s.reader.ReadFileData(rec)
}

func (s *GgpkSource) getBundle(bundleName string) (*bundle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if cached, ok := s.bundleCache[bundleName]; ok {
		return cached, nil
	}

	path := "Bundles2/" + bundleName + ".bundle.bin"
	rec, err := s.reader.FindFile(path)
	if err != nil {
		return nil, fmt.Errorf("finding bundle %s in GGPK: %w", bundleName, err)
	}

	bundleReader := s.reader.FileReaderAt(rec)
	b, err := OpenBundle(bundleReader)
	if err != nil {
		return nil, fmt.Errorf("opening bundle %s from GGPK: %w", bundleName, err)
	}

	s.bundleCache[bundleName] = b
	return b, nil
}

func (s *GgpkSource) ReadFileFromBundle(bundleName string, offset, size uint32) ([]byte, error) {
	b, err := s.getBundle(bundleName)
	if err != nil {
		return nil, err
	}

	data := make([]byte, size)
	if _, err := b.ReadAt(data, int64(offset)); err != nil {
		return nil, fmt.Errorf("reading from bundle %s (offset=%d, size=%d): %w", bundleName, offset, size, err)
	}

	return data, nil
}

func (s *GgpkSource) Close() error {
	return s.reader.Close()
}

func isDirectDATFile(filePath string) (bool, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return false, err
	}
	defer file.Close()

	buffer := make([]byte, 1024)
	n, err := file.Read(buffer)
	if err != nil && err != io.EOF {
		return false, err
	}

	if n < 12 {
		return false, nil
	}

	boundaryMarker := []byte{0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb}
	for i := 4; i <= n-8; i++ {
		if bytes.Equal(buffer[i:i+8], boundaryMarker) {
			rowCount := binary.LittleEndian.Uint32(buffer[0:4])
			if rowCount > 0 && rowCount < 1000000 {
				return true, nil
			}
		}
	}

	return false, nil
}
