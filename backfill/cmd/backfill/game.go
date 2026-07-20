package main

import (
	"fmt"
	"path/filepath"
	"runtime"
)

// game pins the Steam identifiers and version family for each title. The major
// version drives exiledb's path layout (PoE1 data/, PoE2 data/balance/), so a
// patch label of "<major>.<epoch>" both selects the right layout and uniquely
// keys the cache.
type game struct {
	name         string
	title        string // display name used in release titles
	app          int
	contentDepot int
	programDepot int
	major        int
}

var games = map[string]game{
	"poe1": {name: "poe1", title: "PoE1", app: 238960, contentDepot: 238961, programDepot: 238962, major: 3},
	"poe2": {name: "poe2", title: "PoE2", app: 2694490, contentDepot: 2694491, programDepot: 2694492, major: 4},
}

func lookupGame(name string) (game, error) {
	g, ok := games[name]
	if !ok {
		return game{}, fmt.Errorf("unknown game %q (want poe1 or poe2)", name)
	}
	return g, nil
}

// patchLabel is the exiledb --patch equivalent: parts[0] selects the game
// layout, the whole string keys the cache directory.
func (g game) patchLabel(epoch int64) string {
	return fmt.Sprintf("%d.%d", g.major, epoch)
}

// dirs resolves paths relative to the backfill directory, derived from this
// source file's location so the tool works from any working directory.
type dirs struct{ root string }

func newDirs() dirs {
	_, file, _, _ := runtime.Caller(0)
	// <root>/backfill/cmd/backfill/game.go -> <root>/backfill
	return dirs{root: filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))}
}

func (d dirs) contentCatalog(g game) string { return filepath.Join(d.root, g.name+"-content.tsv") }
func (d dirs) programCatalog(g game) string { return filepath.Join(d.root, g.name+"-program.tsv") }
func (d dirs) out(g game) string            { return filepath.Join(d.root, "data", "out", g.name) }
func (d dirs) work() string                 { return filepath.Join(d.root, "data", "work") }
func (d dirs) versionCache(g game) string {
	return filepath.Join(d.root, "data", "versions-"+g.name+".tsv")
}
