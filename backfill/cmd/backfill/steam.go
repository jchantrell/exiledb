package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// errRateLimited means Steam refused further logins for now. It is fatal to a
// run: retrying only extends the penalty. Callers stop and resume after a
// cooldown, which is safe because every step is resumable from what it already
// wrote to disk.
var errRateLimited = errors.New("steam rate limit exceeded")

// steam runs DepotDownloader, the one thing here that genuinely needs an
// external binary. The account must own the game and have a cached session
// (-remember-password); the session token is keyed to the DepotDownloader
// path, so keep it stable.
type steam struct {
	ddl     string
	account string
	work    string
}

func (s steam) validate() error {
	if s.ddl == "" {
		return errors.New("set -ddl (path to DepotDownloader)")
	}
	if _, err := os.Stat(s.ddl); err != nil {
		return fmt.Errorf("DepotDownloader not found at %s: %w", s.ddl, err)
	}
	if s.account == "" {
		return errors.New("set -account (steam login with a cached session)")
	}
	return nil
}

// pull downloads just the depot files matching patterns into dir. Patterns are
// DepotDownloader filelist lines (a bare path, or "regex:...").
func (s steam) pull(ctx context.Context, app, depot int, manifest string, patterns []string, dir string) error {
	if err := os.MkdirAll(s.work, 0o755); err != nil {
		return fmt.Errorf("creating work dir: %w", err)
	}
	list := filepath.Join(s.work, "filelist.txt")
	if err := os.WriteFile(list, []byte(strings.Join(patterns, "\n")+"\n"), 0o644); err != nil {
		return fmt.Errorf("writing filelist: %w", err)
	}

	cmd := exec.CommandContext(ctx, s.ddl,
		"-app", fmt.Sprint(app),
		"-depot", fmt.Sprint(depot),
		"-manifest", manifest,
		"-filelist", list,
		"-dir", dir,
		"-username", s.account,
		"-remember-password",
	)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	cmd.Stdin = nil // never block on a password prompt

	err := cmd.Run()
	if bytes.Contains(out.Bytes(), []byte("RateLimitExceeded")) {
		return errRateLimited
	}
	if err != nil {
		return fmt.Errorf("depotdownloader (depot %d manifest %s): %w\n%s", depot, manifest, err, tail(out.String(), 15))
	}
	return nil
}

func tail(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
