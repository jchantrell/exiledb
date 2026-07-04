package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/spf13/cobra"
)

// ManifestDiff describes file changes between two manifests.
type ManifestDiff struct {
	OldPatch     string   `json:"old_patch,omitempty"`
	NewPatch     string   `json:"new_patch,omitempty"`
	AddedCount   int      `json:"added_count"`
	RemovedCount int      `json:"removed_count"`
	Added        []string `json:"added"`
	Removed      []string `json:"removed"`
}

var diffOutput string

var diffCmd = &cobra.Command{
	Use:   "diff <old-manifest.json> <new-manifest.json>",
	Short: "Diff two manifest files to see what changed between game versions",
	Long: `Diff compares two manifests produced by the manifest command and
outputs the file paths that were added and removed between them as JSON.`,
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		oldManifest, err := readManifest(args[0])
		if err != nil {
			return err
		}
		newManifest, err := readManifest(args[1])
		if err != nil {
			return err
		}

		return writeJSON(diffManifests(oldManifest, newManifest), diffOutput)
	},
}

func readManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading manifest %s: %w", path, err)
	}

	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parsing manifest %s: %w", path, err)
	}
	if m.Files == nil {
		return nil, fmt.Errorf("manifest %s has no files field", path)
	}
	return &m, nil
}

func diffManifests(oldManifest, newManifest *Manifest) *ManifestDiff {
	oldSet := make(map[string]bool, len(oldManifest.Files))
	for _, f := range oldManifest.Files {
		oldSet[f] = true
	}
	newSet := make(map[string]bool, len(newManifest.Files))
	for _, f := range newManifest.Files {
		newSet[f] = true
	}

	added := []string{}
	for f := range newSet {
		if !oldSet[f] {
			added = append(added, f)
		}
	}
	removed := []string{}
	for f := range oldSet {
		if !newSet[f] {
			removed = append(removed, f)
		}
	}
	sort.Strings(added)
	sort.Strings(removed)

	return &ManifestDiff{
		OldPatch:     oldManifest.Patch,
		NewPatch:     newManifest.Patch,
		AddedCount:   len(added),
		RemovedCount: len(removed),
		Added:        added,
		Removed:      removed,
	}
}

func init() {
	rootCmd.AddCommand(diffCmd)
	diffCmd.Flags().StringVarP(&diffOutput, "output", "o", "", "write diff to file instead of stdout")
}
