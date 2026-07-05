// Package upgrade implements self-update against GitHub releases: it checks
// the latest release, downloads the matching binary, and swaps it into place
// atomically.
package upgrade

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var releaseAPIURL = "https://api.github.com/repos/jchantrell/exiledb/releases/latest"

// httpClient bounds the connection phases; total transfer time is governed
// by the caller's context so a multi-MB binary download is not cut short.
var httpClient = &http.Client{
	Transport: &http.Transport{
		ResponseHeaderTimeout: 30 * time.Second,
	},
}

// Asset is a downloadable file attached to a GitHub release.
type Asset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
}

// Release is the subset of a GitHub release needed to upgrade.
type Release struct {
	TagName string  `json:"tag_name"`
	Assets  []Asset `json:"assets"`
}

// Check fetches the latest release from GitHub.
func Check(ctx context.Context) (*Release, error) {
	return fetchLatestRelease(ctx, releaseAPIURL)
}

// Apply downloads the release asset for the current platform and replaces
// the running executable with it.
func Apply(ctx context.Context, rel *Release) error {
	asset, err := findAsset(rel.Assets, runtime.GOOS, runtime.GOARCH)
	if err != nil {
		return err
	}

	exe, err := executablePath()
	if err != nil {
		return fmt.Errorf("locating current executable: %w", err)
	}

	tmp, err := downloadToTemp(ctx, asset.BrowserDownloadURL, filepath.Dir(exe))
	if err != nil {
		return fmt.Errorf("downloading release: %w", err)
	}

	if err := replaceExecutable(exe, tmp); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("installing new binary (you may need elevated permissions): %w", err)
	}
	return nil
}

func fetchLatestRelease(ctx context.Context, url string) (*Release, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GitHub API returned %s", resp.Status)
	}

	var rel Release
	if err := json.NewDecoder(resp.Body).Decode(&rel); err != nil {
		return nil, fmt.Errorf("decoding release response: %w", err)
	}
	if rel.TagName == "" {
		return nil, fmt.Errorf("release response missing tag name")
	}
	return &rel, nil
}

func assetName(goos, goarch string) string {
	name := fmt.Sprintf("exiledb-%s-%s", goos, goarch)
	if goos == "windows" {
		name += ".exe"
	}
	return name
}

func findAsset(assets []Asset, goos, goarch string) (*Asset, error) {
	want := assetName(goos, goarch)
	for i := range assets {
		if assets[i].Name == want {
			return &assets[i], nil
		}
	}
	return nil, fmt.Errorf("no release asset %q available for %s/%s", want, goos, goarch)
}

// parseVersion parses "v1.2.3" (pre-release/build suffixes ignored) into
// major, minor, patch.
func parseVersion(v string) ([3]int, error) {
	var out [3]int
	s := strings.TrimPrefix(strings.TrimSpace(v), "v")
	if i := strings.IndexAny(s, "-+"); i >= 0 {
		s = s[:i]
	}
	parts := strings.Split(s, ".")
	if len(parts) == 0 || len(parts) > 3 || parts[0] == "" {
		return out, fmt.Errorf("invalid version: %q", v)
	}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return out, fmt.Errorf("invalid version: %q", v)
		}
		out[i] = n
	}
	return out, nil
}

// CompareVersions returns -1, 0, or 1 if version a is older than, equal to,
// or newer than version b.
func CompareVersions(a, b string) (int, error) {
	va, err := parseVersion(a)
	if err != nil {
		return 0, err
	}
	vb, err := parseVersion(b)
	if err != nil {
		return 0, err
	}
	for i := range va {
		switch {
		case va[i] < vb[i]:
			return -1, nil
		case va[i] > vb[i]:
			return 1, nil
		}
	}
	return 0, nil
}

func executablePath() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(exe)
}

// downloadToTemp downloads url into a temporary file inside dir so the
// final rename stays on the same filesystem.
func downloadToTemp(ctx context.Context, url, dir string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("bad status: %s", resp.Status)
	}

	f, err := os.CreateTemp(dir, ".exiledb-upgrade-*")
	if err != nil {
		return "", err
	}

	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		os.Remove(f.Name())
		return "", err
	}
	if err := f.Close(); err != nil {
		os.Remove(f.Name())
		return "", err
	}
	if err := os.Chmod(f.Name(), 0o755); err != nil {
		os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}

// replaceExecutable swaps tmp into place at exe. On Windows a running
// executable cannot be overwritten, so the current binary is moved aside
// first; the leftover .old file is removed on a best-effort basis.
func replaceExecutable(exe, tmp string) error {
	if runtime.GOOS != "windows" {
		return os.Rename(tmp, exe)
	}

	old := exe + ".old"
	os.Remove(old)
	if err := os.Rename(exe, old); err != nil {
		return err
	}
	if err := os.Rename(tmp, exe); err != nil {
		os.Rename(old, exe)
		return err
	}
	os.Remove(old)
	return nil
}
