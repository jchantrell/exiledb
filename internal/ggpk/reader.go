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
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty path")
	}

	currentOffset := r.RootOffset

	for i, part := range parts {
		dir, err := readDirectoryRecord(r.file, currentOffset, r.Version)
		if err != nil {
			return nil, fmt.Errorf("reading directory at offset %d: %w", currentOffset, err)
		}

		var foundOffset uint64
		var isFile bool
		found := false

		for _, entry := range dir.Entries {
			header, err := readRecordHeader(r.file, entry.Offset)
			if err != nil {
				continue
			}

			switch header.Tag {
			case TagPDIR:
				subDir, err := readDirectoryRecord(r.file, entry.Offset, r.Version)
				if err != nil {
					continue
				}
				if strings.EqualFold(subDir.Name, part) {
					foundOffset = entry.Offset
					found = true
				}
			case TagFILE:
				file, err := readFileRecord(r.file, entry.Offset, r.Version)
				if err != nil {
					continue
				}
				if strings.EqualFold(file.Name, part) {
					foundOffset = entry.Offset
					isFile = true
					found = true
				}
			}

			if found {
				break
			}
		}

		if !found {
			return nil, fmt.Errorf("path component %q not found in %q", part, path)
		}

		isLastPart := i == len(parts)-1

		if isLastPart {
			if !isFile {
				return nil, fmt.Errorf("%q is a directory, not a file", path)
			}
			rec, err := readFileRecord(r.file, foundOffset, r.Version)
			if err != nil {
				return nil, err
			}
			return &rec, nil
		}

		if isFile {
			return nil, fmt.Errorf("path component %q is a file, not a directory", part)
		}
		currentOffset = foundOffset
	}

	return nil, fmt.Errorf("file not found: %s", path)
}

// ReadFileData reads the raw data bytes of a file record.
func (r *Reader) ReadFileData(rec *FileRecord) ([]byte, error) {
	data := make([]byte, rec.DataLength)
	_, err := r.file.ReadAt(data, int64(rec.DataOffset))
	if err != nil {
		return nil, fmt.Errorf("reading file data at offset %d: %w", rec.DataOffset, err)
	}
	return data, nil
}

// FileReaderAt returns an io.ReaderAt scoped to the file's data region within the GGPK.
func (r *Reader) FileReaderAt(rec *FileRecord) io.ReaderAt {
	return io.NewSectionReader(r.file, int64(rec.DataOffset), int64(rec.DataLength))
}

// ListDirectory returns entries at a given path in the GGPK tree.
func (r *Reader) ListDirectory(path string) (dirs []string, files []string, err error) {
	currentOffset := r.RootOffset

	if path != "" {
		parts := strings.Split(path, "/")
		for _, part := range parts {
			if part == "" {
				continue
			}
			dir, err := readDirectoryRecord(r.file, currentOffset, r.Version)
			if err != nil {
				return nil, nil, fmt.Errorf("reading directory: %w", err)
			}

			found := false
			for _, entry := range dir.Entries {
				header, err := readRecordHeader(r.file, entry.Offset)
				if err != nil {
					continue
				}
				if header.Tag == TagPDIR {
					subDir, err := readDirectoryRecord(r.file, entry.Offset, r.Version)
					if err != nil {
						continue
					}
					if strings.EqualFold(subDir.Name, part) {
						currentOffset = entry.Offset
						found = true
						break
					}
				}
			}
			if !found {
				return nil, nil, fmt.Errorf("directory %q not found", part)
			}
		}
	}

	dir, err := readDirectoryRecord(r.file, currentOffset, r.Version)
	if err != nil {
		return nil, nil, err
	}

	for _, entry := range dir.Entries {
		header, err := readRecordHeader(r.file, entry.Offset)
		if err != nil {
			continue
		}
		switch header.Tag {
		case TagPDIR:
			subDir, err := readDirectoryRecord(r.file, entry.Offset, r.Version)
			if err != nil {
				continue
			}
			dirs = append(dirs, subDir.Name)
		case TagFILE:
			file, err := readFileRecord(r.file, entry.Offset, r.Version)
			if err != nil {
				continue
			}
			files = append(files, file.Name)
		}
	}

	return dirs, files, nil
}
