package bundle

// Index represents a bundle index that can be used to find files and bundles
type Index interface {
	// GetFileInfo returns information about a file, including which bundle contains it
	GetFileInfo(path string) (*FileLocation, error)
	// ListBundles returns all bundle names in the index
	ListBundles() []string
	// ListFiles returns all file paths in the index
	ListFiles() []string
}

// Bundle represents an opened bundle that can be read from
type Bundle interface {
	// Read returns the entire contents of the bundle decompressed
	Read() ([]byte, error)
	// Size returns the uncompressed size of the bundle
	Size() int64
	// ReadAt reads data from a specific offset in the decompressed bundle
	ReadAt(p []byte, off int64) (int, error)
}

// FileLocation contains information about where a file is located in the bundle system
type FileLocation struct {
	BundleName string
	Offset     uint32
	Size       uint32
}

