package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"

	"github.com/spf13/cobra"
)

var manifestCmd = &cobra.Command{
	Use:   "manifest",
	Short: "Dump all file paths in the bundle index, one per line",
	Long: `Manifest traverses the entire bundle index and outputs every file path,
one per line. Output is deterministic (sorted paths), making it suitable
for committing to version control and diffing between game versions with
standard tools like diff and comm.

Use --ggpk to read from a Content.ggpk file instead of downloading from CDN.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		index, err := loadBundleIndex()
		if err != nil {
			return err
		}

		files := index.ListFiles()
		sort.Strings(files)
		files = dedupeNonEmpty(files)

		return writeLines(files)
	},
}

// dedupeNonEmpty removes empty and duplicate paths from a sorted slice.
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

// writeLines writes one line per entry to stdout.
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
	rootCmd.AddCommand(manifestCmd)
}
