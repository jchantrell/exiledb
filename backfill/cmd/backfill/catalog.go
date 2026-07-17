package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

// pairGrace is how long after a content push a program (client) manifest may
// land and still belong to the same patch. Both depots are usually pushed the
// same second; the window covers a client build trailing its content by a
// little without reaching the next patch.
const pairGrace = 900

// entry is one catalog row: a depot manifest and when it was pushed.
type entry struct {
	Epoch    int64
	Date     string
	Manifest string
}

// loadCatalog reads a TSV of epoch<TAB>date<TAB>manifest, skipping the header,
// returning rows sorted oldest first.
func loadCatalog(path string) ([]entry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening catalog: %w", err)
	}
	defer f.Close()

	var rows []entry
	sc := bufio.NewScanner(f)
	for line := 0; sc.Scan(); line++ {
		text := strings.TrimSpace(sc.Text())
		if text == "" || strings.HasPrefix(text, "#") || strings.HasPrefix(text, "epoch\t") {
			continue
		}
		fields := strings.Split(text, "\t")
		if len(fields) != 3 {
			return nil, fmt.Errorf("%s:%d: want 3 tab-separated fields, got %d", path, line+1, len(fields))
		}
		epoch, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s:%d: bad epoch %q: %w", path, line+1, fields[0], err)
		}
		rows = append(rows, entry{Epoch: epoch, Date: fields[1], Manifest: fields[2]})
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("reading catalog: %w", err)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Epoch < rows[j].Epoch })
	return rows, nil
}

// pairPrograms maps each content epoch to the program manifest live at that
// patch: the newest program manifest at or before the content push (plus grace).
func pairPrograms(content, program []entry) map[int64]string {
	paired := make(map[int64]string, len(content))
	for _, c := range content {
		i := sort.Search(len(program), func(i int) bool { return program[i].Epoch > c.Epoch+pairGrace }) - 1
		if i < 0 {
			continue
		}
		paired[c.Epoch] = program[i].Manifest
	}
	return paired
}

// leagues maps "<game>/<major.minor>" to a short league name.
type leagues map[string]string

func loadLeagues(path string) (leagues, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening leagues: %w", err)
	}
	defer f.Close()

	out := leagues{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Split(strings.TrimSpace(sc.Text()), "\t")
		if len(fields) != 3 || fields[0] == "game" {
			continue
		}
		out[fields[0]+"/"+fields[1]] = fields[2]
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("reading leagues: %w", err)
	}
	return out, nil
}

// lookup resolves the league for a client version, keyed on its major.minor.
func (l leagues) lookup(gameName, version string) string {
	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		return ""
	}
	return l[gameName+"/"+parts[0]+"."+parts[1]]
}
