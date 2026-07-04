package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDiffManifests(t *testing.T) {
	oldFiles := []string{"data/a.dat", "data/b.dat", "art/removed.dds"}
	newFiles := []string{"data/b.dat", "data/a.dat", "data/new.dat", "art/added.dds"}

	added, removed := diffManifests(oldFiles, newFiles)

	wantAdded := []string{"art/added.dds", "data/new.dat"}
	if !reflect.DeepEqual(added, wantAdded) {
		t.Errorf("added = %v, want %v", added, wantAdded)
	}
	wantRemoved := []string{"art/removed.dds"}
	if !reflect.DeepEqual(removed, wantRemoved) {
		t.Errorf("removed = %v, want %v", removed, wantRemoved)
	}
}

func TestDiffManifestsIdentical(t *testing.T) {
	files := []string{"data/a.dat"}

	added, removed := diffManifests(files, files)

	if len(added) != 0 || len(removed) != 0 {
		t.Errorf("expected empty diff, got added=%v removed=%v", added, removed)
	}
}

func TestReadLines(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "manifest.txt")
	os.WriteFile(path, []byte("data/a.dat\r\ndata/b.dat\n\ndata/c.dat\n"), 0o644)

	got, err := readLines(path)
	if err != nil {
		t.Fatalf("readLines error: %v", err)
	}
	want := []string{"data/a.dat", "data/b.dat", "data/c.dat"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("readLines = %v, want %v", got, want)
	}

	if _, err := readLines(filepath.Join(dir, "missing.txt")); err == nil {
		t.Error("expected error for missing file")
	}
}
