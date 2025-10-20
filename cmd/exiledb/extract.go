package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/jchantrell/exiledb/internal/bundle"
	"github.com/jchantrell/exiledb/internal/cache"
	"github.com/jchantrell/exiledb/internal/cdn"
	"github.com/jchantrell/exiledb/internal/dat"
	"github.com/jchantrell/exiledb/internal/database"
	"github.com/jchantrell/exiledb/internal/export"
	"github.com/jchantrell/exiledb/internal/utils"
	"github.com/spf13/cobra"
)

type ExtractionStats struct {
	StartTime        time.Time
	EndTime          time.Time
	TotalTables      int
	ProcessedTables  int
	RowsInserted     int64
	ProcessingErrors int
	DatabaseErrors   int
	FilesExported    int
}

var (
	forceDownload bool
	ext           = ".datc64"
)

var extractCmd = &cobra.Command{
	Use:   "extract",
	Short: "Download bundles and extract DAT files into SQLite database",
	Long: `Extract downloads Path of Exile game bundles from CDN servers and
extracts DAT files into a queryable SQLite database.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		stats := &ExtractionStats{
			StartTime: time.Now(),
		}
		var memStatsStart runtime.MemStats
		runtime.ReadMemStats(&memStatsStart)

		showProgress := !(noProgress || cfg.LogFormat == "json" || cfg.LogLevel == "debug")

		slog.Info("Starting extract...", "languages", cfg.Languages)

		dbOptions := &database.DatabaseOptions{
			Path: cfg.Database,
		}

		db, err := database.NewDatabase(dbOptions)
		if err != nil {
			return fmt.Errorf("creating database: %w", err)
		}
		defer db.Close()

		hasTables, err := db.HasUserTables(context.Background())
		if err != nil {
			return fmt.Errorf("checking database tables: %w", err)
		}
		if hasTables {
			return fmt.Errorf("database already contains tables")
		}

		cache := cache.CacheManager()

		gameVersion, err := utils.ParseGameVersion(cfg.Patch)
		if err != nil {
			return fmt.Errorf("parsing game version: %w", err)
		}

		if err := cdn.DownloadIndex(cache, cfg.Patch, gameVersion, forceDownload); err != nil {
			return fmt.Errorf("downloading index file: %w", err)
		}

		requiredBundles, err := bundle.DiscoverRequiredBundles(cache, cfg.Patch, cfg.Languages, cfg.Tables, cfg.Files)
		if err != nil {
			return fmt.Errorf("discovering required bundles: %w", err)
		}

		if len(requiredBundles) == 0 {
			slog.Info("No bundles required for current configuration")
			return nil
		}

		if err := cdn.DownloadBundles(cache, cfg.Patch, gameVersion, requiredBundles, forceDownload, showProgress); err != nil {
			return fmt.Errorf("downloading bundles: %w", err)
		}

		bundleManager, err := bundle.NewBundleManager(cache.GetCacheDir(), cfg.Patch)
		if err != nil {
			return fmt.Errorf("creating bundle manager: %w", err)
		}
		defer bundleManager.Close()

		bundleManager.SetLanguages(cfg.Languages)

		schemaManager, err := dat.NewSchemaManager()
		if err != nil {
			return fmt.Errorf("loading schema manager: %w", err)
		}

		validTables, err := schemaManager.GetValidTablesForVersion(cfg.Patch)
		if err != nil {
			return fmt.Errorf("getting valid tables for version %s: %w", cfg.Patch, err)
		}

		datSchemas := getTableSchemas(validTables, cfg.Tables)

		totalSchemas := len(datSchemas)
		stats.TotalTables = totalSchemas
		if totalSchemas == 0 {
			slog.Info("No tables to process")
			return nil
		}

		totalTables := totalSchemas
		for _, table := range datSchemas {
			for _, column := range table.Columns {
				if column.Name != nil && column.Array && column.References != nil {
					totalTables++
				}
			}
		}

		processingStartTime := time.Now()
		slog.Info("Creating database schemas", "count", totalTables)

		schemaProgress := utils.NewProgress(totalTables, showProgress)
		schemaProgressCallback := func(current int, total int, description string) {
			schemaProgress.Update(current, description)
		}

		ddlManager := database.NewDDLManager(db)
		if err := ddlManager.CreateSchemas(context.Background(), datSchemas, schemaProgressCallback); err != nil {
			schemaProgress.Finish()
			return fmt.Errorf("creating schemas: %w", err)
		}

		schemaProgress.Finish()

		slog.Info("Inserting dat files", "count", totalSchemas)
		bulkInsertOptions := &database.BulkInsertOptions{
			BatchSize:             1000,
			MaxRetries:            3,
			ArrayWarningThreshold: 5000, // Warn for extremely large arrays
		}
		bulkInserter := database.NewBulkInserter(db, bulkInsertOptions)

		insertProgress := utils.NewProgress(totalSchemas, showProgress)
		processedCount := 0
		for _, datSchema := range datSchemas {
			select {
			case <-context.Background().Done():
				slog.Warn("Extraction canceled")
				return fmt.Errorf("extraction canceled")
			default:
			}

			processedCount++
			insertProgress.Update(processedCount, datSchema.Name)
			lowerTableName := strings.ToLower(datSchema.Name)

			for _, language := range cfg.Languages {
				basePath := fmt.Sprintf("data/%s%s", lowerTableName, ext)
				languagePath := fmt.Sprintf("data/%s/%s%s", strings.ToLower(language), lowerTableName, ext)

				langPathExists := bundleManager.FileExists(languagePath)
				basePathExists := bundleManager.FileExists(basePath)

				if !langPathExists && !basePathExists {
					slog.Debug("File does not exist", "lang", languagePath, "base", basePath)
					continue
				}

				path := ""

				if langPathExists {
					path = languagePath
				} else {
					path = basePath
				}

				slog.Debug("Processing DAT file", "path", path, "table", datSchema.Name)

				datData, err := bundleManager.GetFile(path)
				if err != nil {
					slog.Error("Failed to get file from bundle", "path", path, "table", datSchema.Name, "error", err)
					continue
				}

				parser := dat.NewDATParser()

				datReader := bytes.NewReader(datData)
				parsedTable, err := parser.ParseDATFileWithFilename(context.Background(), datReader, path, &datSchema)
				if err != nil || len(parsedTable.Rows) == 0 {
					slog.Error("Failed to parse DAT file", "path", path, "table", datSchema.Name, "size_bytes", len(datData), "error", err)
					stats.ProcessingErrors++
					continue
				}

				rowData := make([]database.RowData, len(parsedTable.Rows))
				for i, row := range parsedTable.Rows {
					rowData[i] = database.RowData{
						Index:  row.Index,
						Values: row.Fields,
					}
				}

				tableData := &database.TableData{
					Schema:   &datSchema,
					Rows:     rowData,
					Language: language,
				}
				if err := bulkInserter.InsertTableData(context.Background(), tableData); err != nil {
					slog.Error("Database insert failed", "table", datSchema.Name, "error", err)
					slog.Error("Failed to insert records", "table", datSchema.Name, "error", err)
					stats.DatabaseErrors++
					continue
				}

				stats.RowsInserted += int64(len(parsedTable.Rows))

			}
			stats.ProcessedTables++
		}

		insertProgress.Finish()

		// Export files if configured
		if len(cfg.Files) > 0 {
			slog.Info("Exporting files", "count", len(cfg.Files))

			// Create output directory for exported files
			outputDir := filepath.Join(".", "files")

			// Create exporter
			exporter := export.NewExporter(bundleManager, outputDir)

			// Create progress bar
			fileProgress := utils.NewProgress(len(cfg.Files), showProgress)
			fileProgressCallback := func(current int, total int, description string) {
				fileProgress.Update(current, description)
			}

			// Export files
			if err := exporter.ExportFiles(cfg.Files, fileProgressCallback); err != nil {
				fileProgress.Finish()
				slog.Error("Failed to export files", "error", err)
			} else {
				fileProgress.Finish()
				stats.FilesExported = len(cfg.Files)
			}
		}

		stats.EndTime = time.Now()

		totalDuration := stats.EndTime.Sub(stats.StartTime)
		processingDuration := stats.EndTime.Sub(processingStartTime)

		var memStatsEnd runtime.MemStats
		runtime.ReadMemStats(&memStatsEnd)
		totalMemoryMB := float64(memStatsEnd.Alloc) / 1024.0 / 1024.0

		var tableProcessingRate, rowInsertionRate float64
		processingSeconds := processingDuration.Seconds()
		if processingSeconds > 0 {
			tableProcessingRate = float64(stats.ProcessedTables) / processingSeconds
			rowInsertionRate = float64(stats.RowsInserted) / processingSeconds
		}
		successRate := float64(stats.ProcessedTables) / float64(stats.TotalTables) * 100

		fmt.Printf("Tables processed: %d/%d (%.1f%%)\n", stats.ProcessedTables, stats.TotalTables, successRate)
		fmt.Printf("Rows inserted: %s\n", utils.Number(stats.RowsInserted))
		fmt.Printf("Files exported: %d\n", stats.FilesExported)
		fmt.Printf("Processing errors: %d\n", stats.ProcessingErrors)
		fmt.Printf("Database errors: %d\n", stats.DatabaseErrors)
		fmt.Printf("Total duration: %.1fms\n", float64(totalDuration.Nanoseconds())/1000000.0)
		fmt.Printf("Processing duration: %.1fms\n", float64(processingDuration.Nanoseconds())/1000000.0)
		fmt.Printf("Processing rate: %.2f tables/sec\n", tableProcessingRate)
		fmt.Printf("Insertion rate: %s rows/sec\n", utils.Rate(rowInsertionRate))
		fmt.Printf("Memory usage: %.2fmb\n", totalMemoryMB)
		fmt.Println("Try running: exiledb query --tables")

		return nil
	},
}

func getTableSchemas(validTables []dat.TableSchema, configuredTables []string) []dat.TableSchema {
	if len(configuredTables) == 0 {
		return validTables
	}

	// Convert configured table names to snake_case for comparison
	configuredSnakeCaseNames := make(map[string]bool)
	for _, tableName := range configuredTables {
		snakeCaseName := utils.ToSnakeCase(tableName)
		configuredSnakeCaseNames[snakeCaseName] = true
		slog.Debug("Configured table mapping", "original", tableName, "snake_case", snakeCaseName)
	}

	// Filter validTables to only include those in the configuration
	filteredTables := make([]dat.TableSchema, 0) // Initialize as empty slice, not nil slice
	for _, table := range validTables {
		snakeCaseTableName := utils.ToSnakeCase(table.Name)
		if configuredSnakeCaseNames[snakeCaseTableName] {
			filteredTables = append(filteredTables, table)
			slog.Debug("Including table", "table", table.Name, "snake_case", snakeCaseTableName)
		}
	}

	return filteredTables
}

func init() {
	rootCmd.AddCommand(extractCmd)
	extractCmd.Flags().BoolVar(&forceDownload, "force", false, "Force re-download bundles even if cached")
}
