package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDiffManifests(t *testing.T) {
	oldManifest := &Manifest{
		Patch: "4.4.0.12",
		Files: []string{"data/a.dat", "data/b.dat", "art/removed.dds"},
	}
	newManifest := &Manifest{
		Patch: "4.4.0.13",
		Files: []string{"data/b.dat", "data/a.dat", "data/new.dat", "art/added.dds"},
	}

	diff := diffManifests(oldManifest, newManifest)

	if diff.OldPatch != "4.4.0.12" || diff.NewPatch != "4.4.0.13" {
		t.Errorf("patches not carried through: %q -> %q", diff.OldPatch, diff.NewPatch)
	}
	wantAdded := []string{"art/added.dds", "data/new.dat"}
	if !reflect.DeepEqual(diff.Added, wantAdded) {
		t.Errorf("added = %v, want %v", diff.Added, wantAdded)
	}
	wantRemoved := []string{"art/removed.dds"}
	if !reflect.DeepEqual(diff.Removed, wantRemoved) {
		t.Errorf("removed = %v, want %v", diff.Removed, wantRemoved)
	}
	if diff.AddedCount != 2 || diff.RemovedCount != 1 {
		t.Errorf("counts = %d added, %d removed; want 2, 1", diff.AddedCount, diff.RemovedCount)
	}
}

func TestDiffManifestsIdentical(t *testing.T) {
	m := &Manifest{Files: []string{"data/a.dat"}}

	diff := diffManifests(m, m)

	if len(diff.Added) != 0 || len(diff.Removed) != 0 {
		t.Errorf("expected empty diff, got added=%v removed=%v", diff.Added, diff.Removed)
	}
}

func TestReadManifest(t *testing.T) {
	dir := t.TempDir()

	valid := filepath.Join(dir, "valid.json")
	os.WriteFile(valid, []byte(`{"format_version":1,"patch":"4.4.0.13","file_count":1,"files":["data/a.dat"]}`), 0o644)

	m, err := readManifest(valid)
	if err != nil {
		t.Fatalf("readManifest(valid) error: %v", err)
	}
	if m.Patch != "4.4.0.13" || len(m.Files) != 1 {
		t.Errorf("unexpected manifest: %+v", m)
	}

	noFiles := filepath.Join(dir, "nofiles.json")
	os.WriteFile(noFiles, []byte(`{"patch":"1.0"}`), 0o644)
	if _, err := readManifest(noFiles); err == nil {
		t.Error("expected error for manifest without files field")
	}

	invalid := filepath.Join(dir, "invalid.json")
	os.WriteFile(invalid, []byte(`not json`), 0o644)
	if _, err := readManifest(invalid); err == nil {
		t.Error("expected error for invalid JSON")
	}

	if _, err := readManifest(filepath.Join(dir, "missing.json")); err == nil {
		t.Error("expected error for missing file")
	}
}
