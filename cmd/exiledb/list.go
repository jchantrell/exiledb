package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/jchantrell/exiledb/internal/bundle"
	"github.com/jchantrell/exiledb/internal/cache"
	"github.com/jchantrell/exiledb/internal/cdn"
	"github.com/jchantrell/exiledb/internal/utils"
	"github.com/spf13/cobra"
)

var listPath string

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List files and directories in the game bundle index",
	Long: `List files and directories at a given path within the game bundle index.
Only downloads the index file — no bundles are fetched.

Use --ggpk to list from a Content.ggpk file instead of downloading from CDN.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		var indexData []byte
		var cachePath string

		if cfg.GgpkPath != "" {
			source, err := bundle.NewGgpkSource(cfg.GgpkPath)
			if err != nil {
				return fmt.Errorf("opening GGPK: %w", err)
			}
			defer source.Close()

			indexData, err = source.ReadIndex()
			if err != nil {
				return fmt.Errorf("reading index from GGPK: %w", err)
			}
			cachePath = source.IndexCachePath()
		} else {
			c := cache.CacheManager()

			gameVersion, err := utils.ParseGameVersion(cfg.Patch)
			if err != nil {
				return fmt.Errorf("parsing game version: %w", err)
			}

			if err := cdn.DownloadIndex(c, cfg.Patch, gameVersion, false); err != nil {
				return fmt.Errorf("downloading index file: %w", err)
			}

			indexData, err = os.ReadFile(c.GetIndexPath(cfg.Patch))
			if err != nil {
				return fmt.Errorf("reading index file: %w", err)
			}
			cachePath = c.GetIndexPath(cfg.Patch) + ".cache"
		}

		index, err := bundle.LoadIndexCached(indexData, cachePath)
		if err != nil {
			return fmt.Errorf("loading index: %w", err)
		}

		prefix := strings.TrimSuffix(listPath, "/")

		var matched []string
		if prefix == "" {
			matched = index.ListFiles()
		} else {
			matched = index.ListFilesWithPrefix(prefix)
		}

		prefixWithSlash := ""
		if prefix != "" {
			prefixWithSlash = prefix + "/"
		}

		dirs := make(map[string]bool)
		var files []string

		for _, f := range matched {
			rest := f[len(prefixWithSlash):]
			if rest == "" {
				continue
			}

			if idx := strings.Index(rest, "/"); idx >= 0 {
				dirs[rest[:idx]] = true
			} else {
				files = append(files, rest)
			}
		}

		if len(dirs) == 0 && len(files) == 0 {
			return fmt.Errorf("no entries found at path: %s", listPath)
		}

		sortedDirs := make([]string, 0, len(dirs))
		for d := range dirs {
			sortedDirs = append(sortedDirs, d)
		}
		sort.Strings(sortedDirs)
		sort.Strings(files)

		for _, d := range sortedDirs {
			fmt.Printf("%s/\n", d)
		}
		for _, f := range files {
			fmt.Println(f)
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(listCmd)
	listCmd.Flags().StringVar(&listPath, "path", "", "path to list contents of")
}
