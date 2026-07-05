package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"

	"github.com/jchantrell/exiledb/internal/bundle"
	"github.com/jchantrell/exiledb/internal/extract"
	"github.com/spf13/cobra"
)

var manifestCmd = &cobra.Command{
	Use:   "manifest",
	Short: "Dump all file paths in the bundle index, one per line",
	Long: `Manifest traverses the entire bundle index and outputs every file path,
one per line. Output is deterministic (sorted paths), making it suitable
for committing to version control and diffing between game versions with
standard tools like diff and comm.

Use --sizes to append each file's uncompressed size in bytes, tab-separated.
Use --ggpk to read from a Content.ggpk file instead of downloading from CDN.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		sizes, err := cmd.Flags().GetBool("sizes")
		if err != nil {
			return err
		}

		index, err := extract.LoadIndex(cmd.Context(), cfg)
		if err != nil {
			return err
		}

		if sizes {
			return writeLines(sizeLines(index.ListFileEntries()))
		}

		files := index.ListFiles()
		sort.Strings(files)
		files = dedupeNonEmpty(files)

		return writeLines(files)
	},
}

func sizeLines(entries []bundle.FileEntry) []string {
	lines := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.Path == "" {
			continue
		}
		lines = append(lines, fmt.Sprintf("%s\t%d", e.Path, e.Size))
	}
	sort.Strings(lines)
	return dedupeNonEmpty(lines)
}

// The bundle index contains a handful of entries with unresolvable names.
func dedupeNonEmpty(sorted []string) []string {
	result := make([]string, 0, len(sorted))
	prev := ""
	for _, f := range sorted {
		if f == "" || f == prev {
			continue
		}
		result = append(result, f)
		prev = f
	}
	return result
}

func writeLines(lines []string) error {
	w := bufio.NewWriter(os.Stdout)
	for _, line := range lines {
		if _, err := w.WriteString(line); err != nil {
			return fmt.Errorf("writing output: %w", err)
		}
		if err := w.WriteByte('\n'); err != nil {
			return fmt.Errorf("writing output: %w", err)
		}
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("writing output: %w", err)
	}
	return nil
}

func init() {
	manifestCmd.Flags().Bool("sizes", false, "append uncompressed size in bytes to each path, tab-separated")
	rootCmd.AddCommand(manifestCmd)
}
