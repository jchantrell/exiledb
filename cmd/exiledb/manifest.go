package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/spf13/cobra"
)

// Manifest is a snapshot of every file path in the game's bundle index.
// Files are sorted so output is deterministic and diffable across versions.
type Manifest struct {
	FormatVersion int      `json:"format_version"`
	Patch         string   `json:"patch,omitempty"`
	FileCount     int      `json:"file_count"`
	Files         []string `json:"files"`
}

const manifestFormatVersion = 1

var manifestOutput string

var manifestCmd = &cobra.Command{
	Use:   "manifest",
	Short: "Dump all file paths in the bundle index as JSON",
	Long: `Manifest traverses the entire bundle index and outputs every file path
as a JSON document. Output is deterministic (sorted paths), making it suitable
for committing to version control and diffing between game versions with
the diff command.

Use --ggpk to read from a Content.ggpk file instead of downloading from CDN.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		index, err := loadBundleIndex()
		if err != nil {
			return err
		}

		files := index.ListFiles()
		sort.Strings(files)
		files = dedupeNonEmpty(files)

		manifest := &Manifest{
			FormatVersion: manifestFormatVersion,
			Patch:         cfg.Patch,
			FileCount:     len(files),
			Files:         files,
		}

		return writeJSON(manifest, manifestOutput)
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

// writeJSON marshals v with indentation and writes it to path,
// or to stdout when path is empty.
func writeJSON(v any, path string) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}
	data = append(data, '\n')

	if path == "" {
		_, err = os.Stdout.Write(data)
		return err
	}

	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}
	return nil
}

func init() {
	rootCmd.AddCommand(manifestCmd)
	manifestCmd.Flags().StringVarP(&manifestOutput, "output", "o", "", "write manifest to file instead of stdout")
}
