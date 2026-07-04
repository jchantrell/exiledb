package main

import (
	"sort"

	"github.com/spf13/cobra"
)

var diffCmd = &cobra.Command{
	Use:   "diff <old-manifest> <new-manifest>",
	Short: "Diff two manifest files to see what changed between game versions",
	Long: `Diff compares two manifests produced by the manifest command and
outputs added paths prefixed with "+" and removed paths prefixed with "-".`,
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		oldFiles, err := readLines(args[0])
		if err != nil {
			return err
		}
		newFiles, err := readLines(args[1])
		if err != nil {
			return err
		}

		added, removed := diffManifests(oldFiles, newFiles)

		lines := make([]string, 0, len(added)+len(removed))
		for _, f := range removed {
			lines = append(lines, "- "+f)
		}
		for _, f := range added {
			lines = append(lines, "+ "+f)
		}

		return writeLines(lines)
	},
}

func diffManifests(oldFiles, newFiles []string) (added, removed []string) {
	oldSet := make(map[string]bool, len(oldFiles))
	for _, f := range oldFiles {
		oldSet[f] = true
	}
	newSet := make(map[string]bool, len(newFiles))
	for _, f := range newFiles {
		newSet[f] = true
	}

	added = []string{}
	for f := range newSet {
		if !oldSet[f] {
			added = append(added, f)
		}
	}
	removed = []string{}
	for f := range oldSet {
		if !newSet[f] {
			removed = append(removed, f)
		}
	}
	sort.Strings(added)
	sort.Strings(removed)
	return added, removed
}

func init() {
	rootCmd.AddCommand(diffCmd)
}
