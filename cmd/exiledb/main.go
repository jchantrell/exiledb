package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jchantrell/exiledb/internal/config"
	"github.com/jchantrell/exiledb/internal/ui"
	"github.com/jchantrell/exiledb/internal/version"
	"github.com/lmittmann/tint"
	"github.com/spf13/cobra"
)

var cfg *config.Config

// flagValues receives the persistent flag values; PersistentPreRunE copies
// them into cfg after Cobra has parsed the command line.
var flagValues config.Config

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
		values := flagValues
		cfg = &values

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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.Version = version.Get()
	flags := rootCmd.PersistentFlags()
	flags.StringVarP(&flagValues.Patch, "patch", "p", "", "patch version to use")
	flags.StringVarP(&flagValues.Database, "database", "d", "exile.db", "database file path")
	flags.StringSliceVar(&flagValues.Tables, "tables", nil, "comma-separated list of tables to extract")
	flags.StringSliceVar(&flagValues.Files, "files", nil, "comma-separated list of files to extract")
	flags.StringSliceVar(&flagValues.Languages, "languages", []string{"English"}, "comma-separated list of languages to extract")
	flags.StringVar(&flagValues.LogLevel, "log-level", "info", "log level (debug, info, warn, error)")
	flags.StringVar(&flagValues.LogFormat, "log-format", "text", "log format (text, json)")
	flags.Bool("no-progress", false, "disable progress bar")
	flags.StringVar(&flagValues.GgpkPath, "ggpk", "", "path to Content.ggpk file (reads from GGPK instead of CDN)")
	flags.StringVar(&flagValues.SchemaPath, "schema", "", "path to a local schema.min.json (default: download latest release)")
}
