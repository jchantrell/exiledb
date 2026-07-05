package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/jchantrell/exiledb/internal/extract"
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
		index, err := extract.LoadIndex(cmd.Context(), cfg)
		if err != nil {
			return err
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
