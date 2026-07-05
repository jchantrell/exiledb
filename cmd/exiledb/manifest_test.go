package main

import (
	"reflect"
	"testing"

	"github.com/jchantrell/exiledb/internal/bundle"
)

func TestDedupeNonEmpty(t *testing.T) {
	got := dedupeNonEmpty([]string{"", "", "a", "a", "b", "c", "c"})
	want := []string{"a", "b", "c"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("dedupeNonEmpty = %v, want %v", got, want)
	}

	if got := dedupeNonEmpty(nil); len(got) != 0 {
		t.Errorf("dedupeNonEmpty(nil) = %v, want empty", got)
	}
}

func TestSizeLines(t *testing.T) {
	entries := []bundle.FileEntry{
		{Path: "data/mods.datc64", Size: 2048},
		{Path: "", Size: 7},
		{Path: "art/icon.dds", Size: 512},
		{Path: "data/mods.datc64", Size: 2048},
	}
	got := sizeLines(entries)
	want := []string{"art/icon.dds\t512", "data/mods.datc64\t2048"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("sizeLines = %v, want %v", got, want)
	}

	if got := sizeLines(nil); len(got) != 0 {
		t.Errorf("sizeLines(nil) = %v, want empty", got)
	}
}
