package main

import (
	"reflect"
	"testing"
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
