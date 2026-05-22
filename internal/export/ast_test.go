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

	if result[0] != 12 {
		t.Fatalf("expected version 12, got %d", result[0])
	}
}

func TestAstHeaderSize(t *testing.T) {
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

	if size >= len(data) {
		t.Fatalf("header size %d >= file size %d", size, len(data))
	}
}
