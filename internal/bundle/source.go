package bundle

import (
	"fmt"
	"io"
	"os"

	"github.com/jchantrell/exiledb/internal/cache"
	"github.com/jchantrell/exiledb/internal/ggpk"
)

type BundleSource interface {
	ReadIndex() ([]byte, error)
	OpenBundle(name string) (io.ReaderAt, io.Closer, error)
	IndexCachePath() string
	Close() error
}

type CacheSource struct {
	patch string
	cache *cache.Cache
}

func NewCacheSource(c *cache.Cache, patch string) *CacheSource {
	return &CacheSource{patch: patch, cache: c}
}

func (s *CacheSource) ReadIndex() ([]byte, error) {
	return os.ReadFile(s.cache.IndexPath(s.patch))
}

func (s *CacheSource) OpenBundle(name string) (io.ReaderAt, io.Closer, error) {
	bundlePath := s.cache.BundlePath(s.patch, name)
	f, err := os.Open(bundlePath)
	if err != nil {
		return nil, nil, fmt.Errorf("opening bundle file %s: %w", bundlePath, err)
	}
	return f, f, nil
}

func (s *CacheSource) IndexCachePath() string {
	return s.cache.IndexPath(s.patch) + ".cache"
}

func (s *CacheSource) Close() error {
	return nil
}

type GgpkSource struct {
	reader   *ggpk.Reader
	ggpkPath string
}

func NewGgpkSource(ggpkPath string) (*GgpkSource, error) {
	r, err := ggpk.Open(ggpkPath)
	if err != nil {
		return nil, fmt.Errorf("opening GGPK file: %w", err)
	}
	return &GgpkSource{reader: r, ggpkPath: ggpkPath}, nil
}

func (s *GgpkSource) ReadIndex() ([]byte, error) {
	rec, err := s.reader.FindFile("Bundles2/_.index.bin")
	if err != nil {
		return nil, fmt.Errorf("finding index in GGPK: %w", err)
	}
	return s.reader.ReadFileData(rec)
}

func (s *GgpkSource) OpenBundle(name string) (io.ReaderAt, io.Closer, error) {
	path := "Bundles2/" + name + ".bundle.bin"
	rec, err := s.reader.FindFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("finding bundle %s in GGPK: %w", name, err)
	}
	return s.reader.FileReaderAt(rec), nil, nil
}

func (s *GgpkSource) IndexCachePath() string {
	return s.ggpkPath + ".idx"
}

func (s *GgpkSource) Close() error {
	return s.reader.Close()
}
