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
Only downloads the index file — no bundles are fetched.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		c := cache.CacheManager()

		gameVersion, err := utils.ParseGameVersion(cfg.Patch)
		if err != nil {
			return fmt.Errorf("parsing game version: %w", err)
		}

		if err := cdn.DownloadIndex(c, cfg.Patch, gameVersion, false); err != nil {
			return fmt.Errorf("downloading index file: %w", err)
		}

		indexData, err := os.ReadFile(c.GetIndexPath(cfg.Patch))
		if err != nil {
			return fmt.Errorf("reading index file: %w", err)
		}

		decompressedData, err := bundle.DecompressIndexBundle(indexData)
		if err != nil {
			return fmt.Errorf("decompressing index: %w", err)
		}

		index, err := bundle.LoadIndex(decompressedData)
		if err != nil {
			return fmt.Errorf("loading index: %w", err)
		}

		prefix := strings.ToLower(strings.TrimSuffix(listPath, "/"))
		if prefix != "" {
			prefix += "/"
		}

		dirs := make(map[string]bool)
		var files []string

		for _, f := range index.ListFiles() {
			if f == "" {
				continue
			}
			lower := strings.ToLower(f)
			if !strings.HasPrefix(lower, prefix) {
				continue
			}

			rest := f[len(prefix):]
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
