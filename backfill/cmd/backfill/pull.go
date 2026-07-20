package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jchantrell/exiledb/internal/cache"
	"github.com/jchantrell/exiledb/internal/extract"
)

const indexFilePattern = `regex:^Bundles2/_\.index\.bin$`

func runPull(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("pull", flag.ExitOnError)
	gameName := fs.String("game", "poe1", "game to pull (poe1|poe2)")
	catalogPath := fs.String("catalog", "", "catalog TSV (default <game>-content.tsv)")
	ddl := fs.String("ddl", os.Getenv("DDL"), "path to DepotDownloader (or $DDL)")
	account := fs.String("account", os.Getenv("ACCOUNT"), "steam login with cached session (or $ACCOUNT)")
	throttle := fs.Duration("throttle", 0, "pause between patches, e.g. 8s, to stay under Steam's login rate limit")
	if err := fs.Parse(args); err != nil {
		return err
	}

	g, err := lookupGame(*gameName)
	if err != nil {
		return err
	}
	d := newDirs()
	s := steam{ddl: *ddl, account: *account, work: d.work()}
	if err := s.validate(); err != nil {
		return err
	}

	if *catalogPath == "" {
		*catalogPath = d.contentCatalog(g)
	}
	catalog, err := loadCatalog(*catalogPath)
	if err != nil {
		return err
	}
	c, err := cache.New()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(d.out(g), 0o755); err != nil {
		return err
	}
	return pullAll(ctx, g, d, s, c, catalog, *throttle)
}

// pullAll walks the catalog oldest to newest so each release can diff against
// the one before it. Releases already on disk are skipped but still carry the
// diff chain forward, which is what makes a rerun resumable.
func pullAll(ctx context.Context, g game, d dirs, s steam, c *cache.Cache, catalog []entry, throttle time.Duration) error {
	var prev *entry
	for i := range catalog {
		e := catalog[i]
		if _, err := os.Stat(filepath.Join(d.out(g), fmt.Sprint(e.Epoch))); err == nil {
			fmt.Printf("skip %d (%s, already built)\n", e.Epoch, e.Date)
			prev = &catalog[i]
			continue
		}

		err := pullPatch(ctx, g, d, s, c, e, prev)
		if errors.Is(err, errRateLimited) {
			fmt.Printf("RATE-LIMITED at %d (%s) — cool down, then rerun to resume\n", e.Epoch, e.Date)
			return nil
		}
		if err != nil {
			return fmt.Errorf("patch %d (%s): %w", e.Epoch, e.Date, err)
		}
		prev = &catalog[i]

		if err := pause(ctx, throttle); err != nil {
			return err
		}
	}
	fmt.Printf("pull complete. artifacts in %s\n", d.out(g))
	return nil
}

// pullPatch builds one release: fetch the index and the bundles holding dat
// tables, extract the artifacts from cache, diff against the previous release,
// then drop the bundles again so disk stays flat across a long run.
func pullPatch(ctx context.Context, g game, d dirs, s steam, c *cache.Cache, e entry, prev *entry) (err error) {
	label := g.patchLabel(e.Epoch)
	outDir := filepath.Join(d.out(g), fmt.Sprint(e.Epoch))
	idxDir := filepath.Join(d.work(), "idx")
	bundleDir := filepath.Join(d.work(), "bundles")

	defer func() {
		os.RemoveAll(c.PatchDir(label))
		os.RemoveAll(idxDir)
		os.RemoveAll(bundleDir)
		if err != nil {
			os.RemoveAll(outDir) // never leave a half-built release behind
		}
	}()
	os.RemoveAll(idxDir)
	os.RemoveAll(bundleDir)

	if err = s.pull(ctx, g.app, g.contentDepot, e.Manifest, []string{indexFilePattern}, idxDir); err != nil {
		return err
	}
	if err = cacheIndex(c, label, idxDir); err != nil {
		return fmt.Errorf("caching index: %w", err)
	}

	index, err := extract.LoadIndex(ctx, patchConfig(label))
	if err != nil {
		return fmt.Errorf("loading index: %w", err)
	}
	if err = s.pull(ctx, g.app, g.contentDepot, e.Manifest, datBundleFiles(index), bundleDir); err != nil {
		return err
	}
	if _, err = cacheBundles(c, label, bundleDir); err != nil {
		return fmt.Errorf("caching bundles: %w", err)
	}

	if err = writeArtifacts(ctx, g, label, outDir, e); err != nil {
		return err
	}

	summary, err := diffAgainst(d, g, prev, outDir)
	if err != nil {
		return err
	}
	fmt.Printf("%s (%d): %s\n", e.Date, e.Epoch, summary)
	return nil
}

// writeArtifacts extracts this patch's release files from the now-populated cache.
func writeArtifacts(ctx context.Context, g game, label, outDir string, e entry) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	if err := writeManifest(ctx, label, filepath.Join(outDir, "manifest.txt")); err != nil {
		return err
	}
	if err := writeDatStats(ctx, label, filepath.Join(outDir, "dat-stats.jsonl")); err != nil {
		return err
	}
	return writeRelease(filepath.Join(outDir, "versions.json"), release{
		Game: g.name, Manifest: e.Manifest, Epoch: e.Epoch, Date: e.Date,
	})
}

// diffAgainst writes this release's file-diff assets relative to the previous
// release. dat-level diffs aren't persisted — they're derivable from any two
// dat-stats.jsonl, so only the summary is reported.
func diffAgainst(d dirs, g game, prev *entry, outDir string) (string, error) {
	manifestPath := filepath.Join(outDir, "manifest.txt")
	statsPath := filepath.Join(outDir, "dat-stats.jsonl")
	if prev == nil {
		stats, err := loadDatStats(statsPath)
		if err != nil {
			return "", err
		}
		files, err := readLines(manifestPath)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("baseline (%d dats, %d files)", len(stats), len(files)), nil
	}

	prevDir := filepath.Join(d.out(g), fmt.Sprint(prev.Epoch))
	added, removed, err := diffManifests(filepath.Join(prevDir, "manifest.txt"), manifestPath)
	if err != nil {
		return "", fmt.Errorf("diffing manifests: %w", err)
	}
	if err := writeLines(filepath.Join(outDir, "added-files.txt"), added); err != nil {
		return "", err
	}
	if err := writeLines(filepath.Join(outDir, "removed-files.txt"), removed); err != nil {
		return "", err
	}

	prevStats, err := loadDatStats(filepath.Join(prevDir, "dat-stats.jsonl"))
	if err != nil {
		return "", err
	}
	curStats, err := loadDatStats(statsPath)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("files +%d/-%d  %s", len(added), len(removed), diffDatStats(prevStats, curStats)), nil
}
