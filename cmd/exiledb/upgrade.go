package main

import (
	"fmt"

	"github.com/jchantrell/exiledb/internal/upgrade"
	"github.com/jchantrell/exiledb/internal/version"
	"github.com/spf13/cobra"
)

var upgradeCheckOnly bool

var upgradeCmd = &cobra.Command{
	Use:   "upgrade",
	Short: "Upgrade exiledb to the latest release",
	Long: `Check GitHub for the latest exiledb release and replace the current
binary with it if a newer version is available.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		current := version.Get()
		if current == "dev" {
			return fmt.Errorf("this binary was built from source without version information; upgrade with git pull or go install")
		}

		rel, err := upgrade.Check(cmd.Context())
		if err != nil {
			return fmt.Errorf("checking for latest release: %w", err)
		}

		cmp, err := upgrade.CompareVersions(current, rel.TagName)
		if err != nil {
			return err
		}
		if cmp >= 0 {
			fmt.Printf("exiledb %s is already the latest version\n", current)
			return nil
		}

		if upgradeCheckOnly {
			fmt.Printf("upgrade available: %s -> %s\nRun 'exiledb upgrade' to install it.\n", current, rel.TagName)
			return nil
		}

		fmt.Printf("Downloading exiledb %s...\n", rel.TagName)
		if err := upgrade.Apply(cmd.Context(), rel); err != nil {
			return err
		}

		fmt.Printf("Upgraded exiledb %s -> %s\n", current, rel.TagName)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(upgradeCmd)
	upgradeCmd.Flags().BoolVar(&upgradeCheckOnly, "check", false, "check for a new version without installing it")
}
