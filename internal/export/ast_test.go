package export

import (
	"os"
	"testing"
)

func TestDecompressAST_V6_Passthrough(t *testing.T) {
	data, err := os.ReadFile("/home/joel/Workspace/exiledb/files/art@models@npc@wounded@woundedmale1@offset.ast")
	if err != nil {
		t.Skip("test file not found")
	}

	result, err := DecompressAST(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result) != len(data) {
		t.Fatalf("v6 file should pass through unchanged, got %d bytes (expected %d)", len(result), len(data))
	}
}

func TestDecompressAST_V9(t *testing.T) {
	data, err := os.ReadFile("/home/joel/Workspace/exiledb/files/art@models@chests@barrels@barrelcluster04.ast")
	if err != nil {
		t.Skip("test file not found")
	}

	result, err := DecompressAST(data)
	if err != nil {
		t.Fatalf("decompression failed: %v", err)
	}

	if len(result) <= len(data) {
		t.Fatalf("decompressed file should be larger, got %d bytes (original %d)", len(result), len(data))
	}

	// Verify header is preserved
	if result[0] != data[0] {
		t.Fatalf("version byte changed")
	}
	if result[1] != data[1] {
		t.Fatalf("bone count changed")
	}
}

func TestDecompressAST_V12(t *testing.T) {
	data, err := os.ReadFile("/home/joel/Workspace/exiledb/files/art@models@monsters@greatsnakeskeletonboss@rig.ast")
	if err != nil {
		t.Skip("test file not found")
	}

	result, err := DecompressAST(data)
	if err != nil {
		t.Fatalf("decompression failed: %v", err)
	}

	if len(result) <= len(data) {
		t.Fatalf("decompressed file should be larger, got %d bytes (original %d)", len(result), len(data))
	}

	if result[0] != 12 {
		t.Fatalf("expected version 12, got %d", result[0])
	}
}

func TestAstHeaderSize(t *testing.T) {
	// v9 file
	data, err := os.ReadFile("/home/joel/Workspace/exiledb/files/art@models@chests@barrels@barrelcluster04.ast")
	if err != nil {
		t.Skip("test file not found")
	}

	size, err := astHeaderSize(data)
	if err != nil {
		t.Fatalf("header parse failed: %v", err)
	}

	if size <= 8 {
		t.Fatalf("header size too small: %d", size)
	}

	// Verify the bytes at the header boundary look like a bundle header
	// (first 4 bytes should be the uncompressed size, a reasonable positive number)
	if size+4 <= len(data) {
		uncSize := int(data[size]) | int(data[size+1])<<8 | int(data[size+2])<<16 | int(data[size+3])<<24
		if uncSize <= 0 || uncSize > 100_000_000 {
			t.Fatalf("bundle uncompressed size at header boundary looks wrong: %d", uncSize)
		}
	}
}
