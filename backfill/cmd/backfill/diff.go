package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

// datStat is the subset of a dat-stats.jsonl record needed to classify a change.
type datStat struct {
	Path     string `json:"path"`
	RowCount int    `json:"row_count"`
	RowWidth int    `json:"row_width"`
	VarSize  int    `json:"var_size"`
	SHA256   string `json:"sha256"`
}

// datSummary counts how tables changed between two patches. The hash decides
// whether a table changed at all; the structural fields say how.
type datSummary struct {
	Total   int
	Added   int
	Removed int
	Changed int
	Schema  int // row width moved: columns changed
	Rows    int // row count moved
	Data    int // variable-length data moved
	Value   int // same shape, different bytes
}

func (s datSummary) String() string {
	return fmt.Sprintf("dats: +%d -%d ~%d (schema=%d rows=%d data=%d value=%d)",
		s.Added, s.Removed, s.Changed, s.Schema, s.Rows, s.Data, s.Value)
}

func loadDatStats(path string) (map[string]datStat, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	out := map[string]datStat{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var d datStat
		if err := json.Unmarshal(sc.Bytes(), &d); err != nil {
			return nil, fmt.Errorf("parsing %s: %w", path, err)
		}
		out[d.Path] = d
	}
	return out, sc.Err()
}

func diffDatStats(prev, cur map[string]datStat) datSummary {
	s := datSummary{Total: len(cur)}
	for p := range prev {
		if _, ok := cur[p]; !ok {
			s.Removed++
		}
	}
	for p, c := range cur {
		o, ok := prev[p]
		if !ok {
			s.Added++
			continue
		}
		if o.SHA256 == c.SHA256 {
			continue
		}
		s.Changed++
		switch {
		case o.RowWidth != c.RowWidth:
			s.Schema++
		case o.RowCount != c.RowCount:
			s.Rows++
		case o.VarSize != c.VarSize:
			s.Data++
		default:
			s.Value++
		}
	}
	return s
}

// diffManifests compares two sorted manifests. Both are written sorted by byte
// value, so a linear merge gives the same answer as comm(1) under LC_ALL=C.
func diffManifests(prevPath, curPath string) (added, removed []string, err error) {
	prev, err := readLines(prevPath)
	if err != nil {
		return nil, nil, err
	}
	cur, err := readLines(curPath)
	if err != nil {
		return nil, nil, err
	}

	i, j := 0, 0
	for i < len(prev) && j < len(cur) {
		switch {
		case prev[i] == cur[j]:
			i++
			j++
		case prev[i] < cur[j]:
			removed = append(removed, prev[i])
			i++
		default:
			added = append(added, cur[j])
			j++
		}
	}
	removed = append(removed, prev[i:]...)
	added = append(added, cur[j:]...)
	return added, removed, nil
}

func readLines(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var lines []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	return lines, sc.Err()
}
