package export

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"unicode/utf16"
)

type transform struct {
	suffixes   []string
	outputName func(path string) string
	write      func(path, outputPath string, data []byte) error
}

var transforms = []transform{
	{
		suffixes:   []string{".dds"},
		outputName: func(path string) string { return strings.TrimSuffix(path, ".dds") + ".png" },
		write:      writeDDSAsPNG,
	},
	{
		suffixes: []string{".txt", ".text"},
		write:    writeDecodedText,
	},
	{
		suffixes: []string{".ast"},
		write:    writeDecompressedAST,
	},
}

func transformFor(path string) transform {
	for _, t := range transforms {
		for _, suffix := range t.suffixes {
			if strings.HasSuffix(path, suffix) {
				return t
			}
		}
	}
	return transform{write: writeRaw}
}

func (t transform) output(path string) string {
	if t.outputName == nil {
		return path
	}
	return t.outputName(path)
}

func writeDDSAsPNG(path, outputPath string, data []byte) error {
	if err := ConvertDDSToPNG(data, nil, outputPath); err != nil {
		slog.Warn("Skipping DDS conversion", "path", path, "error", err)
		return err
	}
	slog.Debug("Converted DDS to PNG", "path", path, "output", outputPath)
	return nil
}

func writeDecodedText(path, outputPath string, data []byte) error {
	text, err := DecodeUTF16LE(data)
	if err != nil {
		slog.Debug("Text file is not UTF-16LE, writing as-is", "path", path, "error", err)
	} else {
		data = []byte(text)
		slog.Debug("Decoded text file to UTF-8", "path", path, "output", outputPath)
	}
	return writeRaw(path, outputPath, data)
}

func writeDecompressedAST(path, outputPath string, data []byte) error {
	decompressed, err := DecompressAST(data)
	if err != nil {
		slog.Warn("AST decompression failed, writing as-is", "path", path, "error", err)
	} else {
		data = decompressed
		slog.Debug("Decompressed AST animation payload", "path", path)
	}
	return writeRaw(path, outputPath, data)
}

func writeRaw(path, outputPath string, data []byte) error {
	if err := os.WriteFile(outputPath, data, 0644); err != nil {
		slog.Error("Failed to write file", "path", outputPath, "error", err)
		return err
	}
	slog.Debug("Copied file", "path", path, "output", outputPath)
	return nil
}

func DecodeUTF16LE(data []byte) (string, error) {
	if len(data)%2 != 0 {
		return "", fmt.Errorf("invalid UTF-16LE data: odd number of bytes")
	}

	u16 := make([]uint16, len(data)/2)
	for i := 0; i < len(u16); i++ {
		u16[i] = uint16(data[i*2]) | uint16(data[i*2+1])<<8
	}

	return string(utf16.Decode(u16)), nil
}
