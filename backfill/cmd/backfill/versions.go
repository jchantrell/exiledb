package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const clientExePattern = `regex:PathOfExileSteam\.exe$`

// buildTagRe matches the build tag baked into the client exe, e.g. "tags/3.28.0j".
// It is the only place a historical client version survives — the CDN keeps
// just the current patch.
var buildTagRe = regexp.MustCompile(`tags/([0-9][0-9.]*[a-z]?)`)

func runVersions(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("versions", flag.ExitOnError)
	gameName := fs.String("game", "poe1", "game to enrich (poe1|poe2)")
	ddl := fs.String("ddl", os.Getenv("DDL"), "path to DepotDownloader (or $DDL)")
	account := fs.String("account", os.Getenv("ACCOUNT"), "steam login with cached session (or $ACCOUNT)")
	throttle := fs.Duration("throttle", 0, "pause between pulls, e.g. 8s, to stay under Steam's login rate limit")
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

	content, paired, err := loadPairing(d, g)
	if err != nil {
		return err
	}

	resolved, err := loadVersionCache(d.versionCache(g))
	if err != nil {
		return err
	}
	limited, err := resolveAll(ctx, g, d, s, paired, resolved, *throttle)
	if err != nil {
		return err
	}

	leagueMap, err := loadLeagues(d.leagues())
	if err != nil {
		return err
	}
	wrote, err := enrichReleases(d, g, content, paired, resolved, leagueMap)
	if err != nil {
		return err
	}
	fmt.Printf("done: %d versions cached, enriched %d releases\n", len(resolved), wrote)
	if limited {
		fmt.Println("(incomplete — rerun after a cooldown to finish)")
	}
	return nil
}

// loadPairing reads both catalogs and maps each content release to the program
// manifest live at its patch.
func loadPairing(d dirs, g game) ([]entry, map[int64]string, error) {
	content, err := loadCatalog(d.contentCatalog(g))
	if err != nil {
		return nil, nil, err
	}
	program, err := loadCatalog(d.programCatalog(g))
	if err != nil {
		return nil, nil, err
	}
	paired := pairPrograms(content, program)
	if len(paired) < len(content) {
		fmt.Printf("warning: %d of %d releases have no program manifest at or before their patch\n",
			len(content)-len(paired), len(content))
	}
	return content, paired, nil
}

// resolveAll pulls every client build not already cached, recording each as it
// goes. Reports whether it stopped early on a rate limit.
func resolveAll(ctx context.Context, g game, d dirs, s steam, paired map[int64]string, resolved map[string]string, throttle time.Duration) (bool, error) {
	fmt.Printf("resolving client versions for %s\n", g.name)
	for _, pm := range uniqueSorted(paired) {
		if _, ok := resolved[pm]; ok {
			continue
		}
		version, err := resolveVersion(ctx, g, d, s, pm)
		if errors.Is(err, errRateLimited) {
			fmt.Println("RATE-LIMITED — cool down, then rerun to resume")
			return true, nil
		}
		if err != nil {
			return false, fmt.Errorf("program manifest %s: %w", pm, err)
		}
		if version == "" {
			fmt.Printf("  %s -> no build tag found\n", pm)
			continue
		}
		resolved[pm] = version
		if err := appendVersionCache(d.versionCache(g), pm, version); err != nil {
			return false, err
		}
		fmt.Printf("  %s -> %s\n", pm, version)

		if err := pause(ctx, throttle); err != nil {
			return false, err
		}
	}
	return false, nil
}

// pause spaces out Steam logins to stay under the rate limit.
func pause(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// resolveVersion pulls one client build and reads its build tag.
func resolveVersion(ctx context.Context, g game, d dirs, s steam, programManifest string) (string, error) {
	exeDir := filepath.Join(d.work(), "exe")
	os.RemoveAll(exeDir)
	defer os.RemoveAll(exeDir)

	if err := s.pull(ctx, g.app, g.programDepot, programManifest, []string{clientExePattern}, exeDir); err != nil {
		return "", err
	}
	exe := filepath.Join(exeDir, "PathOfExileSteam.exe")
	if _, err := os.Stat(exe); err != nil {
		return "", nil // no exe in this build; caller reports and moves on
	}
	return buildTag(exe)
}

// buildTag returns the highest build tag embedded in the exe. A build carries
// both its base version and its point release (e.g. 3.28.0 and 3.28.0j); the
// highest is the actual patch.
func buildTag(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	best := ""
	for _, m := range buildTagRe.FindAllSubmatch(data, -1) {
		if tag := string(m[1]); best == "" || versionLess(best, tag) {
			best = tag
		}
	}
	return best, nil
}

// versionLess orders build tags: numeric components first, then the point-release
// letter, so 3.28.0 < 3.28.0b < 3.28.0j.
func versionLess(a, b string) bool {
	an, al := splitVersion(a)
	bn, bl := splitVersion(b)
	for i := 0; i < len(an) && i < len(bn); i++ {
		if an[i] != bn[i] {
			return an[i] < bn[i]
		}
	}
	if len(an) != len(bn) {
		return len(an) < len(bn)
	}
	return al < bl
}

func splitVersion(v string) ([]int, string) {
	letter := ""
	if n := len(v); n > 0 && v[n-1] >= 'a' && v[n-1] <= 'z' {
		letter, v = v[n-1:], v[:n-1]
	}
	var nums []int
	for _, part := range strings.Split(v, ".") {
		n, err := strconv.Atoi(part)
		if err != nil {
			continue
		}
		nums = append(nums, n)
	}
	return nums, letter
}

// enrichReleases writes client_version, program_manifest and league into each
// release that has a resolved version, leaving the rest untouched.
func enrichReleases(d dirs, g game, content []entry, paired map[int64]string, resolved map[string]string, l leagues) (int, error) {
	wrote := 0
	for _, e := range content {
		pm, ok := paired[e.Epoch]
		if !ok {
			continue
		}
		version, ok := resolved[pm]
		if !ok {
			continue
		}
		path := filepath.Join(d.out(g), fmt.Sprint(e.Epoch), "versions.json")
		r, err := readRelease(path)
		if os.IsNotExist(err) {
			continue // release not pulled yet
		}
		if err != nil {
			return wrote, err
		}
		r.ClientVersion = version
		r.ProgramManifest = pm
		r.League = l.lookup(g.name, version)
		if err := writeRelease(path, r); err != nil {
			return wrote, err
		}
		wrote++
	}
	return wrote, nil
}

func uniqueSorted(paired map[int64]string) []string {
	seen := map[string]struct{}{}
	for _, pm := range paired {
		seen[pm] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for pm := range seen {
		out = append(out, pm)
	}
	sort.Strings(out)
	return out
}

// The version cache makes a run resumable across Steam's rate limits: each
// resolved build is appended immediately, so a rerun only pulls what's missing.
func loadVersionCache(path string) (map[string]string, error) {
	out := map[string]string{}
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if fields := strings.Split(sc.Text(), "\t"); len(fields) == 2 {
			out[fields[0]] = fields[1]
		}
	}
	return out, sc.Err()
}

func appendVersionCache(path, manifest, version string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := fmt.Fprintf(f, "%s\t%s\n", manifest, version); err != nil {
		return err
	}
	return f.Close()
}
