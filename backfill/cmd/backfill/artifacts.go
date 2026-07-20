package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"github.com/jchantrell/exiledb/internal/bundle"
	"github.com/jchantrell/exiledb/internal/cache"
	"github.com/jchantrell/exiledb/internal/config"
	"github.com/jchantrell/exiledb/internal/extract"
	"github.com/jchantrell/exiledb/internal/poe"
)

// release is the versions.json payload, shared with the CI workflow that
// publishes going-forward releases so both eras carry one schema. Fields absent
// on one side are omitted rather than nulled: cdn_version is unrecoverable
// historically, program_manifest has no CI equivalent.
//
// The epoch keys the release; client_version is the human label, added by the
// versions command and absent until then.
type release struct {
	Game            string `json:"game"`
	ClientVersion   string `json:"client_version,omitempty"`
	Date            string `json:"date"`
	Epoch           int64  `json:"epoch"`
	Manifest        string `json:"manifest"`
	ProgramManifest string `json:"program_manifest,omitempty"`
	CDNVersion      string `json:"cdn_version,omitempty"`
}

func writeRelease(path string, r release) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding versions.json: %w", err)
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

func readRelease(path string) (release, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return release{}, err
	}
	var r release
	if err := json.Unmarshal(data, &r); err != nil {
		return release{}, fmt.Errorf("decoding %s: %w", path, err)
	}
	return r, nil
}

// patchConfig mirrors what the exiledb CLI would build for `manifest --patch`.
func patchConfig(label string) *config.Config {
	return &config.Config{Patch: label, Languages: []string{config.LanguageEnglish}}
}

// writeManifest dumps every indexed file path, sorted and deduped — byte-identical
// to `exiledb manifest --patch <label>`.
func writeManifest(ctx context.Context, label, dest string) error {
	index, err := extract.LoadIndex(ctx, patchConfig(label))
	if err != nil {
		return fmt.Errorf("loading index: %w", err)
	}
	files := index.ListFiles()
	sort.Strings(files)
	return writeLines(dest, dedupeNonEmpty(files))
}

// dedupeNonEmpty drops blanks and repeats from a sorted list; the bundle index
// holds a handful of entries with unresolvable names.
func dedupeNonEmpty(sorted []string) []string {
	out := make([]string, 0, len(sorted))
	prev := ""
	for _, s := range sorted {
		if s == "" || s == prev {
			continue
		}
		out = append(out, s)
		prev = s
	}
	return out
}

// writeLines writes one string per line, newline-terminated.
func writeLines(dest string, lines []string) error {
	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for _, line := range lines {
		if _, err := w.WriteString(line + "\n"); err != nil {
			return fmt.Errorf("writing %s: %w", dest, err)
		}
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("writing %s: %w", dest, err)
	}
	return f.Close()
}

// writeDatStats emits per-dat structural metrics — byte-identical to
// `exiledb manifest --patch <label> --stats`.
func writeDatStats(ctx context.Context, label, dest string) error {
	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := extract.WriteDatStats(ctx, patchConfig(label), f); err != nil {
		return fmt.Errorf("writing dat stats: %w", err)
	}
	return nil
}

// datBundleFiles lists the depot paths of every bundle holding a dat table, so
// a pull fetches exactly what --stats needs and never falls back to the CDN
// (which has no historical patches).
func datBundleFiles(index *bundle.Index) []string {
	seen := map[string]struct{}{}
	for _, f := range index.ListFiles() {
		if !slices.Contains(poe.DatExtensions, strings.ToLower(path.Ext(f))) {
			continue
		}
		if loc, err := index.GetFileInfo(f); err == nil {
			seen[loc.BundleName] = struct{}{}
		}
	}
	names := make([]string, 0, len(seen))
	for b := range seen {
		names = append(names, "Bundles2/"+b+".bundle.bin")
	}
	sort.Strings(names)
	return names
}

// cacheIndex installs a pulled _.index.bin so exiledb reads it from cache
// instead of the CDN.
func cacheIndex(c *cache.Cache, label, pulledDir string) error {
	if err := os.MkdirAll(c.PatchDir(label), 0o755); err != nil {
		return err
	}
	return copyFile(filepath.Join(pulledDir, "Bundles2", "_.index.bin"), c.IndexPath(label))
}

// cacheBundles installs pulled bundles under the names the cache itself
// computes. Deriving the destination from cache.BundlePath (rather than
// restating its sanitisation) is what keeps bundle names with slashes or
// spaces — e.g. "Folders/data/8/traditional chinese.dat64" — findable.
func cacheBundles(c *cache.Cache, label, pulledDir string) (int, error) {
	root := filepath.Join(pulledDir, "Bundles2")
	n := 0
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".bundle.bin") {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		name := strings.TrimSuffix(filepath.ToSlash(rel), ".bundle.bin")
		if err := copyFile(p, c.BundlePath(label, name)); err != nil {
			return err
		}
		n++
		return nil
	})
	if os.IsNotExist(err) {
		return 0, nil
	}
	return n, err
}

func copyFile(src, dest string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("copying %s: %w", src, err)
	}
	return out.Close()
}
