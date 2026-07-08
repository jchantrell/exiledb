// Command datbundles prints the DepotDownloader filelist of every bundle that
// holds a .datc64 file for a cached patch, so the backfill driver can pull
// exactly the bundles `manifest --stats` needs (no CDN fallback for historical
// patches). Usage: datbundles <patch-label>
package main

import (
	"fmt"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/jchantrell/exiledb/internal/bundle"
	"github.com/jchantrell/exiledb/internal/cache"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: datbundles <patch-label>")
		os.Exit(2)
	}
	c, err := cache.New()
	if err != nil {
		fmt.Fprintln(os.Stderr, "cache:", err)
		os.Exit(1)
	}
	mgr, err := bundle.NewBundleManager(bundle.NewCacheSource(c, os.Args[1]))
	if err != nil {
		fmt.Fprintln(os.Stderr, "index:", err)
		os.Exit(1)
	}
	idx := mgr.Index()
	seen := map[string]struct{}{}
	for _, f := range idx.ListFiles() {
		if strings.ToLower(path.Ext(f)) != ".datc64" {
			continue
		}
		if loc, err := idx.GetFileInfo(f); err == nil {
			seen[loc.BundleName] = struct{}{}
		}
	}
	names := make([]string, 0, len(seen))
	for b := range seen {
		names = append(names, b)
	}
	sort.Strings(names)
	for _, b := range names {
		fmt.Printf("Bundles2/%s.bundle.bin\n", b)
	}
}
