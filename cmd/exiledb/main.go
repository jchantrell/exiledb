package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/jchantrell/exiledb/internal/config"
	"github.com/jchantrell/exiledb/internal/ui"
	"github.com/jchantrell/exiledb/internal/version"
	"github.com/lmittmann/tint"
	"github.com/spf13/cobra"
)

var cfg *config.Config

// logOutput is the destination for all log output. Commands that render
// progress bars swap it to the bar container so log lines print above live
// bars instead of through them.
var logOutput = ui.NewSwapWriter(os.Stderr)

var rootCmd = &cobra.Command{
	Use:   "exiledb",
	Short: "Path of Exile database extraction and query tool",
	Long: `exiledb is a tool for extracting Path of Exile game data from bundle files
and transforming it into a queryable SQLite database.

This tool downloads bundle files from the PoE CDN, extracts DAT files,
and processes them according to the latest schema to create a local database.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		flags := cmd.Flags()

		cfg = &config.Config{
			Patch:     must(flags.GetString("patch")),
			Database:  must(flags.GetString("database")),
			Tables:    mustSlice(flags.GetStringSlice("tables")),
			Files:     mustSlice(flags.GetStringSlice("files")),
			Languages: mustSlice(flags.GetStringSlice("languages")),
			LogLevel:  must(flags.GetString("log-level")),
			LogFormat: must(flags.GetString("log-format")),
			GgpkPath:  must(flags.GetString("ggpk")),
		}

		var level slog.Level
		switch cfg.LogLevel {
		case "debug":
			level = slog.LevelDebug
		case "info":
			level = slog.LevelInfo
		case "warn":
			level = slog.LevelWarn
		case "error":
			level = slog.LevelError
		default:
			level = slog.LevelInfo
		}

		var handler slog.Handler
		if cfg.LogFormat == "json" {
			handler = slog.NewJSONHandler(logOutput, &slog.HandlerOptions{
				Level: level,
			})
		} else {
			handler = tint.NewHandler(logOutput, &tint.Options{
				Level: level,
			})
		}

		slog.SetDefault(slog.New(handler))

		if cmd.Name() == "version" || cmd.Name() == "upgrade" {
			return nil
		}

		if err := config.Validate(cfg); err != nil {
			return err
		}

		slog.Debug("Configuration",
			"patch", cfg.Patch,
			"database", cfg.Database,
			"languages", cfg.Languages,
			"tables", cfg.Tables,
			"files", cfg.Files,
			"log_level", cfg.LogLevel,
			"log_format", cfg.LogFormat)

		return nil
	},
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.Version = version.Get()
	rootCmd.PersistentFlags().StringP("patch", "p", "", "patch version to use")
	rootCmd.PersistentFlags().StringP("database", "d", "exile.db", "database file path")
	rootCmd.PersistentFlags().StringSlice("tables", nil, "comma-separated list of tables to extract")
	rootCmd.PersistentFlags().StringSlice("files", nil, "comma-separated list of files to extract")
	rootCmd.PersistentFlags().StringSlice("languages", []string{"English"}, "comma-separated list of languages to extract")
	rootCmd.PersistentFlags().String("log-level", "info", "log level (debug, info, warn, error)")
	rootCmd.PersistentFlags().String("log-format", "text", "log format (text, json)")
	rootCmd.PersistentFlags().Bool("no-progress", false, "disable progress bar")
	rootCmd.PersistentFlags().String("ggpk", "", "path to Content.ggpk file (reads from GGPK instead of CDN)")
}

func must(s string, _ error) string          { return s }
func mustSlice(s []string, _ error) []string { return s }
