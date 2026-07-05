package bundle

// FileEntry pairs a file path with its uncompressed size in bytes
type FileEntry struct {
	Path string
	Size uint32
}

// FileLocation contains information about where a file is located in the bundle system
type FileLocation struct {
	BundleName string
	Offset     uint32
	Size       uint32
}
