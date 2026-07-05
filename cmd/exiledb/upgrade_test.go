package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestParseVersion(t *testing.T) {
	tests := []struct {
		in      string
		want    [3]int
		wantErr bool
	}{
		{"v1.5.0", [3]int{1, 5, 0}, false},
		{"1.5.0", [3]int{1, 5, 0}, false},
		{"v2.0", [3]int{2, 0, 0}, false},
		{"v10", [3]int{10, 0, 0}, false},
		{"v1.5.1-0.20240101120000-abcdef123456", [3]int{1, 5, 1}, false},
		{"v1.2.3+build.4", [3]int{1, 2, 3}, false},
		{"", [3]int{}, true},
		{"dev", [3]int{}, true},
		{"v1.x.0", [3]int{}, true},
		{"v1.2.3.4", [3]int{}, true},
	}

	for _, tt := range tests {
		got, err := parseVersion(tt.in)
		if tt.wantErr {
			if err == nil {
				t.Errorf("parseVersion(%q): expected error, got %v", tt.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseVersion(%q): unexpected error: %v", tt.in, err)
			continue
		}
		if got != tt.want {
			t.Errorf("parseVersion(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestCompareVersions(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		{"v1.5.0", "v1.5.0", 0},
		{"v1.5.0", "v1.5.1", -1},
		{"v1.5.1", "v1.5.0", 1},
		{"v1.9.0", "v1.10.0", -1},
		{"v1.5.0", "v2.0.0", -1},
		{"v2.0.0", "v1.99.99", 1},
	}

	for _, tt := range tests {
		got, err := compareVersions(tt.a, tt.b)
		if err != nil {
			t.Errorf("compareVersions(%q, %q): unexpected error: %v", tt.a, tt.b, err)
			continue
		}
		if got != tt.want {
			t.Errorf("compareVersions(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}

	if _, err := compareVersions("dev", "v1.5.0"); err == nil {
		t.Error("compareVersions(dev, v1.5.0): expected error")
	}
}

func TestAssetName(t *testing.T) {
	tests := []struct {
		goos, goarch, want string
	}{
		{"linux", "amd64", "exiledb-linux-amd64"},
		{"windows", "amd64", "exiledb-windows-amd64.exe"},
		{"darwin", "arm64", "exiledb-darwin-arm64"},
	}

	for _, tt := range tests {
		if got := assetName(tt.goos, tt.goarch); got != tt.want {
			t.Errorf("assetName(%q, %q) = %q, want %q", tt.goos, tt.goarch, got, tt.want)
		}
	}
}

func TestFindAsset(t *testing.T) {
	assets := []releaseAsset{
		{Name: "exiledb-linux-amd64", BrowserDownloadURL: "https://example.com/linux"},
		{Name: "exiledb-windows-amd64.exe", BrowserDownloadURL: "https://example.com/windows"},
		{Name: "manifest.txt", BrowserDownloadURL: "https://example.com/manifest"},
	}

	asset, err := findAsset(assets, "linux", "amd64")
	if err != nil {
		t.Fatalf("findAsset(linux/amd64): unexpected error: %v", err)
	}
	if asset.BrowserDownloadURL != "https://example.com/linux" {
		t.Errorf("findAsset(linux/amd64) = %q, want linux URL", asset.BrowserDownloadURL)
	}

	if _, err := findAsset(assets, "darwin", "arm64"); err == nil {
		t.Error("findAsset(darwin/arm64): expected error for missing asset")
	}
}

func TestFetchLatestRelease(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"tag_name":"v1.6.0","assets":[{"name":"exiledb-linux-amd64","browser_download_url":"https://example.com/dl"}]}`))
	}))
	defer srv.Close()

	rel, err := fetchLatestRelease(srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rel.TagName != "v1.6.0" {
		t.Errorf("TagName = %q, want v1.6.0", rel.TagName)
	}
	if len(rel.Assets) != 1 || rel.Assets[0].Name != "exiledb-linux-amd64" {
		t.Errorf("unexpected assets: %+v", rel.Assets)
	}
}

func TestFetchLatestReleaseErrors(t *testing.T) {
	notFound := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer notFound.Close()

	if _, err := fetchLatestRelease(notFound.URL); err == nil {
		t.Error("expected error for 404 response")
	}

	noTag := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"assets":[]}`))
	}))
	defer noTag.Close()

	if _, err := fetchLatestRelease(noTag.URL); err == nil {
		t.Error("expected error for response without tag_name")
	}
}

func TestDownloadToTemp(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("new binary contents"))
	}))
	defer srv.Close()

	dir := t.TempDir()
	tmp, err := downloadToTemp(srv.URL, dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if filepath.Dir(tmp) != dir {
		t.Errorf("temp file %q not in requested dir %q", tmp, dir)
	}

	data, err := os.ReadFile(tmp)
	if err != nil {
		t.Fatalf("reading temp file: %v", err)
	}
	if string(data) != "new binary contents" {
		t.Errorf("temp file contents = %q", data)
	}

	info, err := os.Stat(tmp)
	if err != nil {
		t.Fatalf("stat temp file: %v", err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Errorf("temp file mode = %v, want 0755", info.Mode().Perm())
	}
}

func TestDownloadToTempBadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	if _, err := downloadToTemp(srv.URL, t.TempDir()); err == nil {
		t.Error("expected error for 500 response")
	}
}

func TestReplaceExecutable(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "exiledb")
	tmp := filepath.Join(dir, ".exiledb-upgrade-123")

	if err := os.WriteFile(exe, []byte("old"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tmp, []byte("new"), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := replaceExecutable(exe, tmp); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data, err := os.ReadFile(exe)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "new" {
		t.Errorf("executable contents = %q, want %q", data, "new")
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Errorf("temp file still exists after replace")
	}
}
