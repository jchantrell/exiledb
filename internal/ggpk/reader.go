package ggpk

import (
	"fmt"
	"io"
	"os"
	"strings"
)

type Reader struct {
	file       *os.File
	RootOffset uint64
	Version    uint32
}

func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening GGPK file: %w", err)
	}

	header, err := readRecordHeader(f, 0)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("reading GGPK header: %w", err)
	}

	if header.Tag != TagGGPK {
		f.Close()
		return nil, fmt.Errorf("invalid GGPK signature: expected 0x%X, got 0x%X", TagGGPK, header.Tag)
	}

	rec, err := readGgpkRecord(f, 0)
	if err != nil {
		f.Close()
		return nil, err
	}

	return &Reader{
		file:       f,
		RootOffset: rec.RootOffset,
		Version:    rec.Version,
	}, nil
}

func (r *Reader) Close() error {
	return r.file.Close()
}

// FindFile locates a file by path in the GGPK directory tree.
// Path components are separated by '/'. Lookup is case-insensitive.
func (r *Reader) FindFile(path string) (*FileRecord, error) {
	parts := strings.Split(path, "/")
	last := len(parts) - 1

	currentOffset := r.RootOffset
	for _, part := range parts[:last] {
		dirOffset, file, err := r.findChild(currentOffset, part)
		if err != nil {
			return nil, err
		}
		if dirOffset == 0 && file == nil {
			return nil, fmt.Errorf("path component %q not found in %q", part, path)
		}
		if file != nil {
			return nil, fmt.Errorf("path component %q is a file, not a directory", part)
		}
		currentOffset = dirOffset
	}

	dirOffset, file, err := r.findChild(currentOffset, parts[last])
	if err != nil {
		return nil, err
	}
	if dirOffset == 0 && file == nil {
		return nil, fmt.Errorf("path component %q not found in %q", parts[last], path)
	}
	if file == nil {
		return nil, fmt.Errorf("%q is a directory, not a file", path)
	}
	return file, nil
}

// findChild scans the directory at dirOffset for an entry named name
// (case-insensitive). A directory match returns its offset; a file match
// returns the parsed record. Both zero values mean the name was not found.
func (r *Reader) findChild(dirOffset uint64, name string) (uint64, *FileRecord, error) {
	dir, err := readDirectoryRecord(r.file, dirOffset, r.Version)
	if err != nil {
		return 0, nil, fmt.Errorf("reading directory at offset %d: %w", dirOffset, err)
	}

	for _, entry := range dir.Entries {
		header, err := readRecordHeader(r.file, entry.Offset)
		if err != nil {
			return 0, nil, fmt.Errorf("reading record header at offset %d: %w", entry.Offset, err)
		}

		switch header.Tag {
		case TagPDIR:
			subDir, err := readDirectoryRecord(r.file, entry.Offset, r.Version)
			if err != nil {
				return 0, nil, fmt.Errorf("reading directory record at offset %d: %w", entry.Offset, err)
			}
			if strings.EqualFold(subDir.Name, name) {
				return entry.Offset, nil, nil
			}
		case TagFILE:
			file, err := readFileRecord(r.file, entry.Offset, r.Version)
			if err != nil {
				return 0, nil, fmt.Errorf("reading file record at offset %d: %w", entry.Offset, err)
			}
			if strings.EqualFold(file.Name, name) {
				return 0, &file, nil
			}
		}
	}

	return 0, nil, nil
}

func (r *Reader) ReadFileData(rec *FileRecord) ([]byte, error) {
	data := make([]byte, rec.DataLength)
	_, err := r.file.ReadAt(data, int64(rec.DataOffset))
	if err != nil {
		return nil, fmt.Errorf("reading file data at offset %d: %w", rec.DataOffset, err)
	}
	return data, nil
}

func (r *Reader) FileReaderAt(rec *FileRecord) io.ReaderAt {
	return io.NewSectionReader(r.file, int64(rec.DataOffset), int64(rec.DataLength))
}
