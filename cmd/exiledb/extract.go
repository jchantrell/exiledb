package main

import (
	"log/slog"
	"os"

	"github.com/jchantrell/exiledb/internal/extract"
	"github.com/jchantrell/exiledb/internal/ui"
	"github.com/spf13/cobra"
)

var forceDownload bool

var extractCmd = &cobra.Command{
	Use:   "extract",
	Short: "Download bundles and extract DAT files into SQLite database",
	Long: `Extract downloads Path of Exile game bundles from CDN servers and
extracts DAT files into a queryable SQLite database.

Use --ggpk to extract directly from a Content.ggpk file instead of downloading from CDN.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		noProgress, _ := cmd.Flags().GetBool("no-progress")
		showProgress := !(noProgress || cfg.LogFormat == "json" || cfg.LogLevel == "debug")

		progress := ui.NewProgress(showProgress)
		logOutput.Swap(progress.LogWriter())
		defer progress.Close()
		defer logOutput.Swap(os.Stderr)

		slog.Info("Starting extract...", "languages", cfg.Languages)

		stats, err := extract.Run(cmd.Context(), cfg, extract.Options{
			ForceDownload: forceDownload,
			Progress:      progress.Phase,
		})
		if err != nil {
			return err
		}
		if stats == nil {
			return nil
		}

		stats.Report(os.Stdout)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(extractCmd)
	extractCmd.Flags().BoolVar(&forceDownload, "force", false, "Force re-download bundles even if cached")
}
