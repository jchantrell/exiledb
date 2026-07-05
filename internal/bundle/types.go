package bundle

type FileEntry struct {
	Path string
	Size uint32
}

type FileLocation struct {
	BundleName string
	Offset     uint32
	Size       uint32
}
