package bundle

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/oriath-net/gooz"
)

type bundle struct {
	data        io.ReaderAt
	size        int64
	granularity int64 // size of each chunk of uncompressed data, usually 256KiB
	blocks      []bundleBlock
}

// descriptions of compressed blocks relative to bundle.data
type bundleBlock struct {
	offset int64
	length int64
}

type bundleHead struct {
	UncompressedSize             uint32
	TotalPayloadSize             uint32
	HeadPayloadSize              uint32
	FirstFileEncode              uint32
	_                            uint32
	UncompressedSize2            int64
	TotalPayloadSize2            int64
	BlockCount                   uint32
	UncompressedBlockGranularity uint32
	_                            [4]uint32
}

func OpenBundle(r io.ReaderAt) (*bundle, error) {
	rs := io.NewSectionReader(r, 0, 1<<24)

	var bh bundleHead
	if err := binary.Read(rs, binary.LittleEndian, &bh); err != nil {
		return nil, fmt.Errorf("failed to read bundle head: %w", err)
	}

	blockSizes := make([]uint32, bh.BlockCount)
	if err := binary.Read(rs, binary.LittleEndian, &blockSizes); err != nil {
		return nil, fmt.Errorf("failed to read bundle block sizes (BlockCount=%d): %w", bh.BlockCount, err)
	}

	blocks := make([]bundleBlock, bh.BlockCount)
	p := int64(binary.Size(bh) + binary.Size(blockSizes))
	for i := range blockSizes {
		sz := int64(blockSizes[i])
		blocks[i] = bundleBlock{offset: p, length: sz}
		p += sz
	}

	b := bundle{
		data:        r,
		size:        bh.UncompressedSize2,
		granularity: int64(bh.UncompressedBlockGranularity),
		blocks:      blocks,
	}

	// do a quick sanity check here
	if b.granularity == 0 {
		return nil, fmt.Errorf("granularity is 0?!")
	}

	expectedBlocks := b.size / b.granularity
	if b.size%b.granularity > 0 {
		expectedBlocks += 1
	}

	if int(expectedBlocks) != len(blocks) {
		return nil, fmt.Errorf(
			"got %d blocks of size %d for %d bytes data",
			len(blocks),
			b.granularity,
			b.size,
		)
	}

	return &b, nil
}

func (b *bundle) Size() int64 {
	return b.size
}

func (b *bundle) ReadAt(p []byte, off int64) (int, error) {
	if off+int64(len(p)) > b.size {
		// FIXME: This could be handled more gracefully
		return 0, fmt.Errorf("read outside bounds of file")
	}

	// Temporary buffers for compressed and decompressed data
	ibuf := make([]byte, b.granularity+64)
	obuf := make([]byte, b.granularity)

	n := 0
	for n < len(p) {
		blkId := int(off / b.granularity)
		blkOff := int(off % b.granularity)
		blk := &b.blocks[blkId]

		rawSize := int(b.granularity)
		if blkId == len(b.blocks)-1 {
			rawSize = int(b.size - int64(blkId)*b.granularity)
		}

		oodleBlk := ibuf[:blk.length]
		if n, err := b.data.ReadAt(oodleBlk, blk.offset); n != len(oodleBlk) {
			return 0, err
		}

		_, err := gooz.Decompress(oodleBlk, obuf[:rawSize])
		if err != nil {
			return 0, fmt.Errorf("decompression failed: %w", err)
		}

		copied := copy(p[n:], obuf[blkOff:])
		n += copied
		off += int64(copied)
	}

	return n, nil
}

// Read returns the entire contents of the bundle decompressed
func (b *bundle) Read() ([]byte, error) {
	data := make([]byte, b.size)
	_, err := b.ReadAt(data, 0)
	if err != nil {
		return nil, fmt.Errorf("reading bundle data: %w", err)
	}
	return data, nil
}

// bundleFS implements a filesystem interface over the bundle system
type bundleFS struct {
	lower fs.FS
	index bundleIndex
}

func NewLoader(lower fs.FS) (*bundleFS, error) {
	indexFile, err := lower.Open("Bundles2/_.index.bin")
	if err != nil {
		return nil, err
	}

	// FIXME: It'd be neat to defer this until it's needed.
	idx, err := loadBundleIndex(indexFile.(io.ReaderAt))
	if err != nil {
		return nil, err
	}

	return &bundleFS{
		lower: lower,
		index: idx,
	}, nil
}

func (b *bundleFS) Open(name string) (fs.File, error) {
	files := b.index.files

	// super special case
	if name == "." {
		return &bundleFsDir{
			fs:     b,
			prefix: "",
			offset: 0,
		}, nil
	}

	// binary search for the file
	idx := sort.Search(len(b.index.files), func(i int) bool {
		return files[i].path >= name
	})

	if idx < len(files) && files[idx].path == name {
		return &bundleFsFile{
			fs:   b,
			info: &files[idx],
		}, nil
	}

	// check for a directory separately
	dirName := name + "/"
	idx += sort.Search(len(b.index.files)-idx, func(i int) bool {
		return files[idx+i].path >= dirName
	})

	if idx < len(files) && strings.HasPrefix(files[idx].path, dirName) {
		return &bundleFsDir{
			fs:     b,
			prefix: dirName,
			offset: idx,
		}, nil
	}

	// nope, nothing here
	return nil, &fs.PathError{
		Op:   "open",
		Path: name,
		Err:  fs.ErrNotExist,
	}
}

// bundleFsFile implements fs.File for files in bundles
type bundleFsFile struct {
	fs     *bundleFS
	info   *bundleFileInfo
	reader *io.SectionReader
}

func (bff *bundleFsFile) initReader() error {
	if bff.reader != nil {
		return nil
	}

	bundlePath := "Bundles2/" + bff.fs.index.bundles[bff.info.bundleId] + ".bundle.bin"
	bundleFile, err := bff.fs.lower.Open(bundlePath)
	if err != nil {
		return &fs.PathError{
			Op:   "open",
			Path: bff.info.path,
			Err:  fmt.Errorf("unable to open bundle %s: %w", bundlePath, err),
		}
	}

	bundle, err := OpenBundle(bundleFile.(io.ReaderAt))
	if err != nil {
		return &fs.PathError{
			Op:   "open",
			Path: bff.info.path,
			Err:  fmt.Errorf("unable to load bundle %s: %w", bundlePath, err),
		}
	}

	bff.reader = io.NewSectionReader(
		bundle,
		int64(bff.info.offset),
		int64(bff.info.size),
	)

	return nil
}

func (bff *bundleFsFile) Read(p []byte) (int, error) {
	err := bff.initReader()
	if err != nil {
		return 0, err
	}
	return bff.reader.Read(p)
}

func (bff *bundleFsFile) Close() error {
	return nil
}

func (bff *bundleFsFile) Stat() (fs.FileInfo, error) {
	return &bundleFsFileInfo{bff}, nil
}

// bundleFsFileInfo implements fs.FileInfo for bundle files
type bundleFsFileInfo struct {
	*bundleFsFile
}

func (bffi bundleFsFileInfo) Name() string {
	return path.Base(bffi.info.path)
}

func (bffi bundleFsFileInfo) Size() int64 {
	return int64(bffi.info.size)
}

func (bffi bundleFsFileInfo) Mode() fs.FileMode {
	return 0o444
}

func (bffi bundleFsFileInfo) ModTime() time.Time {
	return time.Unix(0, 0)
}

func (bffi bundleFsFileInfo) IsDir() bool {
	return false
}

func (bffi bundleFsFileInfo) Sys() any {
	return nil
}

// bundleFsDir implements fs.File for directories in bundles
type bundleFsDir struct {
	fs     *bundleFS
	prefix string
	offset int
}

func (bfd *bundleFsDir) Read(p []byte) (int, error) {
	return 0, fmt.Errorf("is a directory")
}

func (bfd *bundleFsDir) Close() error {
	return nil
}

func (bfd *bundleFsDir) Stat() (fs.FileInfo, error) {
	return &bundleFsDirInfo{bfd}, nil
}

func (bfd *bundleFsDir) ReadDir(n int) ([]fs.DirEntry, error) {
	files := bfd.fs.index.files
	prefixLen := len(bfd.prefix)

	dirents := []fs.DirEntry{}

	for {
		if bfd.offset >= len(files) {
			break
		}

		fi := &files[bfd.offset]
		if !strings.HasPrefix(fi.path, bfd.prefix) {
			break
		}

		slashIdx := strings.Index(fi.path[prefixLen:], "/")
		if slashIdx != -1 {
			dir := fi.path[:prefixLen+slashIdx]
			dirents = append(dirents, &bundleFsDirEnt{
				fs:   bfd.fs,
				path: dir,
			})
			next := bfd.offset + sort.Search(len(files)-bfd.offset, func(i int) bool {
				return files[bfd.offset+i].path >= dir+"/\xff"
			})
			bfd.offset = next

		} else {
			dirents = append(dirents, &bundleFsDirEnt{
				fs:   bfd.fs,
				path: fi.path,
				file: &bundleFsFile{
					fs:     bfd.fs,
					info:   fi,
					reader: nil, // not needed here
				},
			})
			bfd.offset += 1
		}

		if n > 0 && len(dirents) >= n {
			return dirents, nil
		}
	}

	if n > 0 {
		return dirents, io.EOF
	}

	return dirents, nil
}

// bundleFsDirInfo implements fs.FileInfo for bundle directories
type bundleFsDirInfo struct {
	*bundleFsDir
}

func (bfdi bundleFsDirInfo) Name() string {
	return path.Base(bfdi.prefix)
}

func (bfdi bundleFsDirInfo) Size() int64 {
	return 0
}

func (bfdi bundleFsDirInfo) Mode() fs.FileMode {
	return 0o444 | fs.ModeDir
}

func (bfdi bundleFsDirInfo) ModTime() time.Time {
	return time.Unix(0, 0)
}

func (bfdi bundleFsDirInfo) IsDir() bool {
	return true
}

func (bfdi bundleFsDirInfo) Sys() any {
	return nil
}

// bundleFsDirEnt implements fs.DirEntry for bundle directory entries
type bundleFsDirEnt struct {
	fs   *bundleFS
	path string
	file *bundleFsFile
}

func (bfde *bundleFsDirEnt) Name() string {
	return path.Base(bfde.path)
}

func (bfde *bundleFsDirEnt) IsDir() bool {
	return bfde.file == nil
}

func (bfde *bundleFsDirEnt) Type() fs.FileMode {
	if bfde.IsDir() {
		return 0o444 | fs.ModeDir
	} else {
		return 0o444
	}
}

func (bfde *bundleFsDirEnt) Info() (fs.FileInfo, error) {
	if bfde.IsDir() {
		return &bundleFsDirInfo{
			&bundleFsDir{
				fs:     bfde.fs,
				prefix: bfde.path,
				offset: -1, // unused
			},
		}, nil
	} else {
		return &bundleFsFileInfo{bfde.file}, nil
	}
}

