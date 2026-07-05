// Package extract implements the extraction pipeline: resolve a bundle
// source, pull the requested tables into SQLite, and export requested files.
package extract

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"path/filepath"
	"slices"
	"time"

	"github.com/jchantrell/exiledb/internal/bundle"
	"github.com/jchantrell/exiledb/internal/cache"
	"github.com/jchantrell/exiledb/internal/cdn"
	"github.com/jchantrell/exiledb/internal/config"
	"github.com/jchantrell/exiledb/internal/dat"
	"github.com/jchantrell/exiledb/internal/database"
	"github.com/jchantrell/exiledb/internal/export"
	"github.com/jchantrell/exiledb/internal/poe"
	"github.com/jchantrell/exiledb/internal/ui"
)

// Options carries per-invocation settings that are not part of the config.
type Options struct {
	// ForceDownload re-downloads bundles even when cached.
	ForceDownload bool

	// Progress renders phase progress bars; must not be nil.
	Progress *ui.Progress
}

// Run executes the extraction pipeline. A nil Stats with nil error means
// there was nothing to do for the requested configuration.
func Run(ctx context.Context, cfg *config.Config, opts Options) (*Stats, error) {
	stats := &Stats{StartTime: time.Now()}

	db, err := database.NewDatabase(database.DefaultDatabaseOptions(cfg.Database))
	if err != nil {
		return nil, fmt.Errorf("creating database: %w", err)
	}
	defer db.Close()

	hasTables, err := db.HasUserTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("checking database tables: %w", err)
	}
	if hasTables {
		return nil, fmt.Errorf("database already contains tables")
	}

	manager, err := openSource(ctx, cfg, opts)
	if err != nil {
		return nil, err
	}
	if manager == nil {
		return nil, nil
	}
	defer manager.Close()

	stats.processingStart = time.Now()

	if len(cfg.Tables) > 0 {
		if err := insertTables(ctx, cfg, db, manager, opts.Progress, stats); err != nil {
			return nil, err
		}
		reportForeignKeys(ctx, db)
	}

	if len(cfg.Files) > 0 {
		exportFiles(cfg, manager, opts.Progress, stats)
	}

	stats.EndTime = time.Now()
	return stats, nil
}

// openSource resolves the bundle manager for the configured source: a local
// GGPK archive, or the CDN cache after downloading whatever the requested
// tables/files need. Returns (nil, nil) when no bundles are required.
func openSource(ctx context.Context, cfg *config.Config, opts Options) (*bundle.BundleManager, error) {
	if cfg.GgpkPath != "" {
		slog.Info("Using GGPK file", "path", cfg.GgpkPath)
		source, err := bundle.NewGgpkSource(cfg.GgpkPath)
		if err != nil {
			return nil, fmt.Errorf("opening GGPK: %w", err)
		}
		manager, err := bundle.NewBundleManager(source)
		if err != nil {
			return nil, fmt.Errorf("creating bundle manager from GGPK: %w", err)
		}
		return manager, nil
	}

	c, err := cache.New()
	if err != nil {
		return nil, err
	}

	gameVersion, err := poe.ParseGameVersion(cfg.Patch)
	if err != nil {
		return nil, fmt.Errorf("parsing game version: %w", err)
	}

	if err := cdn.DownloadIndex(ctx, c, cfg.Patch, gameVersion, opts.ForceDownload); err != nil {
		return nil, fmt.Errorf("downloading index file: %w", err)
	}

	manager, err := bundle.NewBundleManager(bundle.NewCacheSource(c, cfg.Patch))
	if err != nil {
		return nil, fmt.Errorf("creating bundle manager: %w", err)
	}

	requiredBundles := bundle.DiscoverRequiredBundles(manager.Index(), cfg.Patch, cfg.Languages, cfg.Tables, cfg.Files)
	if len(requiredBundles) == 0 {
		slog.Info("No bundles required for current configuration")
		manager.Close()
		return nil, nil
	}

	if err := cdn.DownloadBundles(ctx, c, cfg.Patch, gameVersion, requiredBundles, opts.ForceDownload, opts.Progress.Phase()); err != nil {
		manager.Close()
		return nil, fmt.Errorf("downloading bundles: %w", err)
	}

	// Sprite images live inside sheet DDS files whose paths are only known
	// after the sprite index files (downloaded above) are parsed, so sheets
	// need a second discovery pass.
	sheets, err := export.ResolveSpriteSheets(manager, cfg.Files)
	if err != nil {
		slog.Warn("Failed to resolve sprite sheets", "error", err)
	} else if len(sheets) > 0 {
		sheetBundles := bundle.DiscoverRequiredBundles(manager.Index(), cfg.Patch, nil, nil, sheets)
		if err := cdn.DownloadBundles(ctx, c, cfg.Patch, gameVersion, sheetBundles, opts.ForceDownload, opts.Progress.Phase()); err != nil {
			manager.Close()
			return nil, fmt.Errorf("downloading sprite sheet bundles: %w", err)
		}
	}

	return manager, nil
}

// insertTables creates schemas for the requested tables and loads every
// requested language of each table into the database.
func insertTables(ctx context.Context, cfg *config.Config, db *database.Database, manager *bundle.BundleManager, progress *ui.Progress, stats *Stats) error {
	schemaManager, err := dat.NewSchemaManager()
	if err != nil {
		return fmt.Errorf("loading schema manager: %w", err)
	}

	validTables, err := schemaManager.GetValidTablesForVersion(cfg.Patch)
	if err != nil {
		return fmt.Errorf("getting valid tables for version %s: %w", cfg.Patch, err)
	}

	datSchemas := filterTables(validTables, cfg.Tables)
	stats.TotalTables = len(datSchemas)

	totalTables := len(datSchemas)
	for _, table := range datSchemas {
		for _, column := range table.Columns {
			if column.Name != nil && column.Array && column.References != nil {
				totalTables++
			}
		}
	}
	slog.Info("Creating database schemas", "count", totalTables)

	ddlManager := database.NewDDLManager(db)
	if err := ddlManager.CreateSchemas(ctx, datSchemas, progress.Phase()); err != nil {
		return fmt.Errorf("creating schemas: %w", err)
	}

	slog.Info("Inserting dat files", "count", len(datSchemas))
	bulkInserter := database.NewBulkInserter(db, nil)

	insertProgress := progress.Phase()
	for i, datSchema := range datSchemas {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("extraction canceled: %w", err)
		}

		insertProgress(i+1, len(datSchemas), datSchema.Name)

		for _, language := range cfg.Languages {
			path := poe.DatPath(cfg.Patch, datSchema.Name, poe.DatExtension)
			if language != "English" {
				path = poe.DatLangPath(cfg.Patch, language, datSchema.Name, poe.DatExtension)
			}
			if !manager.FileExists(path) {
				slog.Debug("File does not exist", "path", path)
				continue
			}

			slog.Debug("Processing DAT file", "path", path, "table", datSchema.Name)

			datData, err := manager.GetFile(path)
			if err != nil {
				slog.Error("Failed to get file from bundle", "path", path, "table", datSchema.Name, "error", err)
				continue
			}

			parser := dat.NewDATParser()
			parsedTable, err := parser.ParseDATFileWithFilename(ctx, bytes.NewReader(datData), path, &datSchema)
			if err != nil {
				slog.Error("Failed to parse DAT file", "path", path, "table", datSchema.Name, "size_bytes", len(datData), "error", err)
				stats.ProcessingErrors++
				continue
			}
			if len(parsedTable.Rows) == 0 {
				slog.Debug("Table has no rows", "path", path, "table", datSchema.Name)
				continue
			}

			rowData := make([]database.RowData, len(parsedTable.Rows))
			for i, row := range parsedTable.Rows {
				rowData[i] = database.RowData{Index: row.Index, Values: row.Fields}
			}

			tableData := &database.TableData{
				Schema:   &datSchema,
				Rows:     rowData,
				Language: language,
			}
			if err := bulkInserter.InsertTableData(ctx, tableData); err != nil {
				slog.Error("Failed to insert records", "table", datSchema.Name, "error", err)
				stats.DatabaseErrors++
				continue
			}

			stats.RowsInserted += int64(len(parsedTable.Rows))
		}
		stats.ProcessedTables++
	}

	return nil
}

// reportForeignKeys surfaces referential problems after the load; FK
// constraints are documentation-only and never enforced during insertion.
func reportForeignKeys(ctx context.Context, db *database.Database) {
	violations, err := db.CheckForeignKeys(ctx)
	if err != nil {
		slog.Warn("Foreign key check failed", "error", err)
		return
	}
	if len(violations) == 0 {
		return
	}
	perTable := make(map[string]int)
	for _, v := range violations {
		perTable[v.Table]++
	}
	for _, table := range slices.Sorted(maps.Keys(perTable)) {
		slog.Warn("Foreign key violations", "table", table, "count", perTable[table])
	}
}

// exportFiles writes the requested files to ./files. Export failures are
// reported but do not abort the run; the stats reflect what was actually
// exported.
func exportFiles(cfg *config.Config, manager *bundle.BundleManager, progress *ui.Progress, stats *Stats) {
	expandedFiles := manager.SortByBundle(manager.ExpandFilePaths(cfg.Files))
	slog.Info("Exporting files", "requested", len(cfg.Files), "resolved", len(expandedFiles))

	exporter := export.NewExporter(manager, filepath.Join(".", "files"))

	exported, err := exporter.ExportFiles(expandedFiles, progress.Phase())
	if err != nil {
		slog.Error("Failed to export files", "error", err)
	}
	stats.FilesExported = exported
}

// filterTables narrows the valid tables to the configured set, matching on
// the snake_case form so users can spell names either way.
func filterTables(validTables []dat.TableSchema, configuredTables []string) []dat.TableSchema {
	if len(configuredTables) == 0 {
		return validTables
	}

	configured := make(map[string]bool, len(configuredTables))
	for _, tableName := range configuredTables {
		configured[poe.ToSnakeCase(tableName)] = true
	}

	filtered := make([]dat.TableSchema, 0)
	for _, table := range validTables {
		if configured[poe.ToSnakeCase(table.Name)] {
			filtered = append(filtered, table)
		}
	}
	return filtered
}
