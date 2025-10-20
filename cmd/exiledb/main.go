package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/jchantrell/exiledb/internal/config"
	"github.com/lmittmann/tint"
	"github.com/spf13/cobra"
)

var (
	cfg     *config.Config
	cfgFile string

	patch      string
	dbPath     string
	tables     []string
	files      []string
	languages  []string
	logLevel   string
	logFormat  string
	noProgress bool
)

var rootCmd = &cobra.Command{
	Use:   "exiledb",
	Short: "Path of Exile database extraction and query tool",
	Long: `exiledb is a tool for extracting Path of Exile game data from bundle files
and transforming it into a queryable SQLite database.

This tool downloads bundle files from the PoE CDN, extracts DAT files,
and processes them according to the latest schema to create a local database.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		var err error
		cfg, err = config.Load(cfgFile)
		if err != nil {
			return fmt.Errorf("failed to load configuration: %w", err)
		}

		if cmd.Flags().Changed("patch") {
			cfg.Patch = patch
		}
		if cmd.Flags().Changed("database") {
			cfg.Database = dbPath
		}
		if cmd.Flags().Changed("tables") {
			cfg.Tables = tables
		}
		if cmd.Flags().Changed("files") {
			cfg.Files = files
		}
		if cmd.Flags().Changed("languages") {
			cfg.Languages = languages
		}
		if cmd.Flags().Changed("log-level") {
			cfg.LogLevel = logLevel
		}
		if cmd.Flags().Changed("log-format") {
			cfg.LogFormat = logFormat
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
			handler = slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
				Level: level,
			})
		} else {
			handler = tint.NewHandler(os.Stderr, &tint.Options{
				Level: level,
			})
		}

		logger := slog.New(handler)
		slog.SetDefault(logger)

		slog.Info("Configuration",
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
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is exiledb.yaml in pwd)")
	rootCmd.PersistentFlags().StringVarP(&patch, "patch", "p", "", "patch version to use")
	rootCmd.PersistentFlags().StringVarP(&dbPath, "database", "d", "", "database file path")
	rootCmd.PersistentFlags().StringSliceVar(&tables, "tables", []string{}, "comma-separated list of tables to extract")
	rootCmd.PersistentFlags().StringSliceVar(&files, "files", []string{}, "comma-separated list of files to extract")
	rootCmd.PersistentFlags().StringSliceVar(&languages, "languages", []string{"English"}, "comma-separated list of languages to extract")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "", "log level (debug, info, warn, error)")
	rootCmd.PersistentFlags().StringVar(&logFormat, "log-format", "", "log format (text, json)")
	rootCmd.PersistentFlags().BoolVar(&noProgress, "no-progress", false, "disable progress bar")
}
