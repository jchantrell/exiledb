package cdn

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// httpClient bounds the connection phases; total transfer time is governed
// by the caller's context so large bundle downloads are not cut short.
var httpClient = &http.Client{
	Transport: &http.Transport{
		ResponseHeaderTimeout: 30 * time.Second,
	},
}

// Download fetches url into dest atomically: the body streams to a temp file
// in dest's directory which is renamed into place only on success, so an
// interrupted download can never leave a truncated file at dest.
func Download(ctx context.Context, url, dest string) (err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("building request for %s: %w", url, err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("requesting %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("downloading %s: unexpected status %s", url, resp.Status)
	}

	tmp, err := os.CreateTemp(filepath.Dir(dest), filepath.Base(dest)+".tmp*")
	if err != nil {
		return fmt.Errorf("creating temp file for %s: %w", dest, err)
	}
	defer func() {
		if err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
		}
	}()

	if _, err = io.Copy(tmp, resp.Body); err != nil {
		return fmt.Errorf("downloading %s: %w", url, err)
	}
	if err = tmp.Close(); err != nil {
		return fmt.Errorf("closing temp file for %s: %w", dest, err)
	}
	if err = os.Rename(tmp.Name(), dest); err != nil {
		return fmt.Errorf("moving download into place at %s: %w", dest, err)
	}
	return nil
}
