package extract

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"os"
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
)

type Options struct {
	ForceDownload bool

	Progress func() func(done, total int, label string)
}

func (o Options) phase() func(done, total int, label string) {
	if o.Progress == nil {
		return func(int, int, string) {}
	}
	return o.Progress()
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

	gameVersion := 0
	if cfg.GgpkPath == "" || len(cfg.Tables) > 0 {
		gameVersion, err = poe.ParseGameVersion(cfg.Patch)
		if err != nil {
			return nil, fmt.Errorf("parsing game version: %w", err)
		}
	}

	var resolvedTables []dat.TableSchema
	if len(cfg.Tables) > 0 {
		schema, err := loadCommunitySchema(ctx)
		if err != nil {
			return nil, fmt.Errorf("loading community schema: %w", err)
		}
		resolvedTables = filterTables(schema.GetValidTables(gameVersion), cfg.Tables)
	}

	manager, err := openSource(ctx, cfg, opts, gameVersion, resolvedTables)
	if err != nil {
		return nil, err
	}
	if manager == nil {
		return nil, nil
	}
	defer manager.Close()

	stats.processingStart = time.Now()

	if len(resolvedTables) > 0 {
		if err := insertTables(ctx, cfg, db, manager, opts, stats, resolvedTables); err != nil {
			return nil, err
		}
		reportForeignKeys(ctx, db)
	}

	if len(cfg.Files) > 0 {
		if err := exportFiles(ctx, cfg, manager, opts, stats); err != nil {
			return stats, err
		}
	}

	stats.EndTime = time.Now()

	if n := stats.ProcessingErrors + stats.DatabaseErrors; n > 0 {
		return stats, fmt.Errorf("extraction completed with %d table errors", n)
	}
	return stats, nil
}

type source struct {
	bundleSource bundle.BundleSource
	cache        *cache.Cache
	gameVersion  int
}

func resolveSource(ctx context.Context, cfg *config.Config, gameVersion int, force bool) (*source, error) {
	if cfg.GgpkPath != "" {
		slog.Info("Using GGPK file", "path", cfg.GgpkPath)
		s, err := bundle.NewGgpkSource(cfg.GgpkPath)
		if err != nil {
			return nil, fmt.Errorf("opening GGPK: %w", err)
		}
		return &source{bundleSource: s}, nil
	}

	c, err := cache.New()
	if err != nil {
		return nil, err
	}

	if err := cdn.DownloadIndex(ctx, c, cfg.Patch, gameVersion, force); err != nil {
		return nil, fmt.Errorf("downloading index file: %w", err)
	}

	return &source{
		bundleSource: bundle.NewCacheSource(c, cfg.Patch),
		cache:        c,
		gameVersion:  gameVersion,
	}, nil
}

func LoadIndex(ctx context.Context, cfg *config.Config) (*bundle.Index, error) {
	gameVersion := 0
	if cfg.GgpkPath == "" {
		var err error
		gameVersion, err = poe.ParseGameVersion(cfg.Patch)
		if err != nil {
			return nil, fmt.Errorf("parsing game version: %w", err)
		}
	}

	src, err := resolveSource(ctx, cfg, gameVersion, false)
	if err != nil {
		return nil, err
	}
	defer src.bundleSource.Close()

	return bundle.LoadIndex(src.bundleSource)
}

func openSource(ctx context.Context, cfg *config.Config, opts Options, gameVersion int, tables []dat.TableSchema) (*bundle.BundleManager, error) {
	src, err := resolveSource(ctx, cfg, gameVersion, opts.ForceDownload)
	if err != nil {
		return nil, err
	}

	manager, err := bundle.NewBundleManager(src.bundleSource)
	if err != nil {
		src.bundleSource.Close()
		return nil, fmt.Errorf("creating bundle manager: %w", err)
	}

	if src.cache == nil {
		return manager, nil
	}
	c := src.cache
	index := manager.Index()

	paths := append(datFilePaths(cfg.Patch, tables, cfg.Languages), index.ExpandFilePaths(cfg.Files)...)
	requiredBundles := bundlesForFiles(index, paths)
	if len(requiredBundles) == 0 {
		slog.Info("No bundles required for current configuration")
		manager.Close()
		return nil, nil
	}

	if err := cdn.DownloadBundles(ctx, c, cfg.Patch, gameVersion, requiredBundles, opts.ForceDownload, opts.phase()); err != nil {
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
		sheetBundles := bundlesForFiles(index, sheets)
		if err := cdn.DownloadBundles(ctx, c, cfg.Patch, gameVersion, sheetBundles, opts.ForceDownload, opts.phase()); err != nil {
			manager.Close()
			return nil, fmt.Errorf("downloading sprite sheet bundles: %w", err)
		}
	}

	return manager, nil
}

func loadCommunitySchema(ctx context.Context) (*dat.CommunitySchema, error) {
	c, err := cache.New()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(c.Dir(), 0755); err != nil {
		return nil, fmt.Errorf("creating schema cache directory: %w", err)
	}
	if err := cdn.Download(ctx, dat.CommunitySchemaURL, c.SchemaPath()); err != nil {
		return nil, fmt.Errorf("downloading schema: %w", err)
	}

	file, err := os.Open(c.SchemaPath())
	if err != nil {
		return nil, fmt.Errorf("opening cached schema: %w", err)
	}
	defer file.Close()

	return dat.ParseCommunitySchema(file)
}

func insertTables(ctx context.Context, cfg *config.Config, db *database.Database, manager *bundle.BundleManager, opts Options, stats *Stats, datSchemas []dat.TableSchema) error {
	stats.TotalTables = len(datSchemas)

	plans, err := database.Plan(datSchemas)
	if err != nil {
		return fmt.Errorf("planning tables: %w", err)
	}

	createdTables, err := database.CreateSchemas(ctx, db, plans, opts.phase())
	if err != nil {
		return fmt.Errorf("creating schemas: %w", err)
	}
	slog.Info("Creating database schemas", "count", createdTables)

	slog.Info("Inserting dat files", "count", len(datSchemas))

	insertProgress := opts.phase()
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

			parsedTable, err := dat.Parse(ctx, datData, &datSchema)
			if err != nil {
				slog.Error("Failed to parse DAT file", "path", path, "table", datSchema.Name, "size_bytes", len(datData), "error", err)
				stats.ProcessingErrors++
				continue
			}
			if len(parsedTable.Rows) == 0 {
				slog.Debug("Table has no rows", "path", path, "table", datSchema.Name)
				continue
			}

			tableData := &database.TableData{
				Schema:   &datSchema,
				Rows:     parsedTable.Rows,
				Language: language,
			}
			if err := database.InsertTableData(ctx, db, plans[i], tableData); err != nil {
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

func exportFiles(ctx context.Context, cfg *config.Config, manager *bundle.BundleManager, opts Options, stats *Stats) error {
	expandedFiles := manager.SortByBundle(manager.ExpandFilePaths(cfg.Files))
	slog.Info("Exporting files", "requested", len(cfg.Files), "resolved", len(expandedFiles))

	exporter := export.NewExporter(manager, filepath.Join(".", "files"))

	exported, err := exporter.ExportFiles(ctx, expandedFiles, opts.phase())
	stats.FilesExported = exported
	if err != nil {
		return fmt.Errorf("exporting files: %w", err)
	}
	return nil
}

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
