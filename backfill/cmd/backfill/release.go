package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Assets are published as-is except the manifest, which is far larger than the
// rest and compresses ~15x. GitHub serves every release asset as a download
// rather than a preview, so gzipping costs a consumer one gunzip and nothing else.
const manifestAsset = "manifest.txt.gz"

func runRelease(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("release", flag.ExitOnError)
	gameName := fs.String("game", "poe1", "game to publish (poe1|poe2)")
	repo := fs.String("repo", os.Getenv("REPO"), "target repo owner/name (or $REPO)")
	publish := fs.Bool("publish", false, "actually create releases (default: dry run)")
	limit := fs.Int("limit", 0, "publish at most N releases (0 = all), for batching")
	if err := fs.Parse(args); err != nil {
		return err
	}

	g, err := lookupGame(*gameName)
	if err != nil {
		return err
	}
	if *publish && *repo == "" {
		return fmt.Errorf("set -repo owner/name (or $REPO) to publish")
	}
	d := newDirs()
	catalog, err := loadCatalog(d.contentCatalog(g))
	if err != nil {
		return err
	}

	if !*publish {
		fmt.Println("DRY RUN — nothing will be created. Re-run with -publish to apply.")
	}
	done, skipped, err := releaseAll(ctx, g, d, catalog, *repo, *publish, *limit)
	if err != nil {
		return err
	}

	verb := "would publish"
	if *publish {
		verb = "published"
	}
	fmt.Printf("\n%s %d releases (%d already present)\n", verb, done, skipped)
	return nil
}

// releaseAll walks the catalog oldest to newest so each release's notes can
// diff against the one before it. Already-published tags are skipped, which is
// what lets an interrupted upload resume.
func releaseAll(ctx context.Context, g game, d dirs, catalog []entry, repo string, publish bool, limit int) (done, skipped int, err error) {
	for i, e := range catalog {
		outDir := filepath.Join(d.out(g), fmt.Sprint(e.Epoch))
		if _, err := os.Stat(outDir); err != nil {
			continue // not pulled
		}
		var prevDir string
		if i > 0 {
			prevDir = filepath.Join(d.out(g), fmt.Sprint(catalog[i-1].Epoch))
		}

		rel, err := renderRelease(g, outDir, prevDir)
		if err != nil {
			return done, skipped, fmt.Errorf("rendering %d: %w", e.Epoch, err)
		}

		if !publish {
			printDryRun(outDir, rel)
		} else {
			exists, err := releaseExists(ctx, repo, rel.tag)
			if err != nil {
				return done, skipped, err
			}
			if exists {
				skipped++
				continue
			}
			if err := publishRelease(ctx, repo, outDir, d.work(), rel); err != nil {
				return done, skipped, fmt.Errorf("publishing %s: %w", rel.tag, err)
			}
			fmt.Printf("published %s  %s\n", rel.tag, rel.title)
		}

		done++
		if limit > 0 && done >= limit {
			return done, skipped, nil
		}
	}
	return done, skipped, nil
}

// renderedRelease is everything GitHub needs for one release.
type renderedRelease struct {
	tag    string
	title  string
	body   string
	assets []string // file names within the release directory
}

func renderRelease(g game, outDir, prevDir string) (renderedRelease, error) {
	r, err := readRelease(filepath.Join(outDir, "versions.json"))
	if err != nil {
		return renderedRelease{}, err
	}

	// client_version names the release because it is the one label the CI
	// workflow can also derive for future patches; the epoch keys it because
	// client versions repeat across silent content hotfixes.
	label := r.ClientVersion
	if label == "" {
		label = r.Date
	}
	rel := renderedRelease{
		tag:    fmt.Sprintf("data-%s-%d", g.name, r.Epoch),
		title:  fmt.Sprintf("%s %s (%s)", g.title, label, r.Date),
		assets: []string{manifestAsset, "dat-stats.jsonl", "versions.json"},
	}

	body, err := renderBody(g, r, outDir, prevDir)
	if err != nil {
		return renderedRelease{}, err
	}
	rel.body = body

	for _, f := range []string{"added-files.txt", "removed-files.txt"} {
		if _, err := os.Stat(filepath.Join(outDir, f)); err == nil {
			rel.assets = append(rel.assets, f)
		}
	}
	return rel, nil
}

func renderBody(g game, r release, outDir, prevDir string) (string, error) {
	files, err := countLines(filepath.Join(outDir, "manifest.txt"))
	if err != nil {
		return "", err
	}
	stats, err := loadDatStats(filepath.Join(outDir, "dat-stats.jsonl"))
	if err != nil {
		return "", err
	}

	var b strings.Builder
	fmt.Fprintf(&b, "File manifest for %s patch %s (%s).\n\n", g.title, r.ClientVersion, r.Date)
	fmt.Fprintf(&b, "Reconstructed from Steam content manifest `%s` — the CDN only serves the\n"+
		"current patch, so historical data is recovered from the depot instead.\n\n", r.Manifest)

	prev, prevErr := readRelease(filepath.Join(prevDir, "versions.json"))
	if prevDir == "" || prevErr != nil {
		fmt.Fprintf(&b, "- Total files: %d\n", files)
		fmt.Fprintf(&b, "- Dat tables: %d (`dat-stats.jsonl`)\n\n", len(stats))
		b.WriteString("First release in the series — no previous release to diff against.\n")
		return b.String(), nil
	}

	added, err := countLines(filepath.Join(outDir, "added-files.txt"))
	if err != nil {
		return "", err
	}
	removed, err := countLines(filepath.Join(outDir, "removed-files.txt"))
	if err != nil {
		return "", err
	}
	prevStats, err := loadDatStats(filepath.Join(prevDir, "dat-stats.jsonl"))
	if err != nil {
		return "", err
	}
	s := diffDatStats(prevStats, stats)

	fmt.Fprintf(&b, "Compared against %s (%s):\n", prev.ClientVersion, prev.Date)
	fmt.Fprintf(&b, "- Total files: %d\n", files)
	fmt.Fprintf(&b, "- Added: %d (`added-files.txt`)\n", added)
	fmt.Fprintf(&b, "- Removed: %d (`removed-files.txt`)\n", removed)
	fmt.Fprintf(&b, "- Dat tables: %d — %d added, %d removed, %d changed (%d value-only) (`dat-stats.jsonl`)\n",
		s.Total, s.Added, s.Removed, s.Changed, s.Value)
	return b.String(), nil
}

func printDryRun(outDir string, rel renderedRelease) {
	fmt.Printf("── %s\n   title: %s\n", rel.tag, rel.title)
	for _, line := range strings.Split(strings.TrimRight(rel.body, "\n"), "\n") {
		fmt.Printf("   │ %s\n", line)
	}
	fmt.Print("   assets:")
	for _, a := range rel.assets {
		// The manifest is compressed at publish time, so on disk it is still
		// the raw file — label the size as such rather than implying the
		// upload is that large.
		if a == manifestAsset {
			if fi, err := os.Stat(filepath.Join(outDir, "manifest.txt")); err == nil {
				fmt.Printf(" %s(raw %s)", a, humanSize(fi.Size()))
				continue
			}
		}
		if fi, err := os.Stat(filepath.Join(outDir, a)); err == nil {
			fmt.Printf(" %s(%s)", a, humanSize(fi.Size()))
			continue
		}
		fmt.Printf(" %s(?)", a)
	}
	fmt.Println()
}

func publishRelease(ctx context.Context, repo, outDir, workDir string, rel renderedRelease) error {
	gzPath := filepath.Join(workDir, manifestAsset)
	if err := gzipFile(filepath.Join(outDir, "manifest.txt"), gzPath); err != nil {
		return fmt.Errorf("gzipping manifest: %w", err)
	}
	defer os.Remove(gzPath)

	notes := filepath.Join(workDir, "notes.md")
	if err := os.WriteFile(notes, []byte(rel.body), 0o644); err != nil {
		return err
	}
	defer os.Remove(notes)

	// --latest=false keeps data releases from displacing the code release.
	args := []string{"release", "create", rel.tag,
		"--repo", repo, "--title", rel.title, "--notes-file", notes, "--latest=false"}
	for _, a := range rel.assets {
		if a == manifestAsset {
			args = append(args, gzPath)
			continue
		}
		args = append(args, filepath.Join(outDir, a))
	}

	cmd := exec.CommandContext(ctx, "gh", args...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("gh release create: %w\n%s", err, tail(out.String(), 10))
	}
	return nil
}

func releaseExists(ctx context.Context, repo, tag string) (bool, error) {
	cmd := exec.CommandContext(ctx, "gh", "release", "view", tag, "--repo", repo)
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err := cmd.Run(); err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return false, nil // absent
		}
		return false, fmt.Errorf("checking %s: %w", tag, err)
	}
	return true, nil
}

func gzipFile(src, dest string) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
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

	zw, err := gzip.NewWriterLevel(out, gzip.BestCompression)
	if err != nil {
		return err
	}
	if _, err := io.Copy(zw, in); err != nil {
		return err
	}
	if err := zw.Close(); err != nil {
		return err
	}
	return out.Close()
}

// countLines streams rather than reading the file in — manifests run to
// millions of lines.
func countLines(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	buf := make([]byte, 256*1024)
	n := 0
	for {
		read, err := f.Read(buf)
		n += bytes.Count(buf[:read], []byte{'\n'})
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return 0, err
		}
	}
}

func humanSize(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(n)/float64(div), "KMGT"[exp])
}
