package main

import (
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
}

var (
	forceDownload bool
)

var extractCmd = &cobra.Command{
	Use:   "extract",
	Short: "Download bundles and extract DAT files into SQLite database",
	Long: `Extract downloads Path of Exile game bundles from CDN servers (if needed) and 
extracts DAT files into a queryable SQLite database. This unified command replaces 
the previous separate fetch and extract workflow.

By default, the command intelligently uses cached bundles when available and downloads 
missing bundles as needed. Use --force to re-download all bundles.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		stats := &ExtractionStats{
			StartTime: time.Now(),
		}

		var memStatsStart runtime.MemStats
		runtime.ReadMemStats(&memStatsStart)

		slog.Info("Starting extract...", "languages", cfg.Languages)

		cache := cache.CacheManager()

		gameVersion, err := utils.ParseGameVersion(cfg.Patch)
		if err != nil {
			return fmt.Errorf("parsing game version: %w", err)
		}

		if err := cdn.DownloadIndex(cache, cfg.Patch, gameVersion, forceDownload); err != nil {
			return fmt.Errorf("downloading index file: %w", err)
		}

		requiredBundles, err := bundle.DiscoverRequiredBundles(cache, cfg.Patch, cfg.Languages, cfg.Tables, cfg.AllTables)
		if err != nil {
			return fmt.Errorf("discovering required bundles: %w", err)
		}

		if len(requiredBundles) == 0 {
			slog.Info("No bundles required for current configuration")
			return nil
		}

		if err := cdn.DownloadBundles(cache, cfg.Patch, gameVersion, requiredBundles, forceDownload); err != nil {
			return fmt.Errorf("downloading bundles: %w", err)
		}

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

		cacheDir := filepath.Dir(cache.GetIndexPath(cfg.Patch))
		cacheDir = filepath.Dir(cacheDir)

		fmt.Println(cacheDir)

		bundleManager, err := bundle.NewManager(cacheDir, cfg.Patch)
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

		tableSchemas := getTableSchemas(validTables, cfg.AllTables, cfg.Tables)

		stats.TotalTables = len(tableSchemas)
		processingStartTime := time.Now()

		ddlManager := database.NewDDLManager(db)
		slog.Info("Creating database schemas", "count", len(tableSchemas))
		if err := ddlManager.CreateSchemas(context.Background(), tableSchemas); err != nil {
			return fmt.Errorf("creating schemas: %w", err)
		}

		var tables []string
		for _, table := range tableSchemas {
			tables = append(tables, table.Name)
		}

		if len(tables) == 0 {
			slog.Info("No tables to process")
			return nil
		}

		slog.Info("Processing tables", "count", len(tables))

		bulkInsertOptions := &database.BulkInsertOptions{
			BatchSize:                 1000,
			MaxRetries:                3,
			MaxJunctionTableArraySize: 50000, // Allow very large legitimate arrays while preventing explosion
			ArrayWarningThreshold:     5000,  // Warn for extremely large arrays
		}
		bulkInserter := database.NewBulkInserter(db, bulkInsertOptions)

		progress := utils.NewProgress(len(tables), !(noProgress || cfg.LogFormat == "json" || cfg.LogLevel == "debug"))

		processedCount := 0
		for _, tableName := range tables {
			select {
			case <-context.Background().Done():
				slog.Warn("Extraction canceled")
				return fmt.Errorf("extraction canceled")
			default:
			}

			processedCount++
			progress.Update(processedCount, tableName)
			lowerTableName := strings.ToLower(tableName)
			ext := ".datc64"
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

				slog.Debug("Processing DAT file", "path", path, "table", tableName)

				datData, err := bundleManager.GetFile(path)
				if err != nil {
					slog.Error("Failed to get file from bundle", "path", path, "table", tableName, "error", err)
					continue
				}

				tableSchema, err := schemaManager.GetTableSchemaForVersion(tableName, cfg.Patch)
				if err != nil || tableSchema == nil {
					slog.Debug("Skipping table - no schema found or not compatible with game version",
						"table", tableName,
						"version", cfg.Patch,
						"error", err)
					continue
				}

				parserOptions := &dat.ParserOptions{
					StrictMode:                 false,
					ValidateReferences:         false,
					MaxStringLength:            65536, // 64KB max string length
					MaxArrayCount:              65536, // Restored to match reference implementations
					MaxJunctionTableArrayCount: 65536, // Allow large arrays with content validation
					ArraySizeWarningThreshold:  1000,  // Warn when arrays exceed 1000 elements
				}
				parser := dat.NewDATParser(parserOptions)

				datReader := strings.NewReader(string(datData))
				parsedTable, err := parser.ParseDATFileWithFilename(context.Background(), datReader, path, tableSchema)
				if err != nil {
					slog.Error("Failed to parse DAT file", "path", path, "table", tableName, "size_bytes", len(datData), "error", err)
					stats.ProcessingErrors++
					continue
				}

				if len(parsedTable.Rows) > 0 {
					rowData := make([]database.RowData, len(parsedTable.Rows))
					for i, row := range parsedTable.Rows {
						rowData[i] = database.RowData{
							Index:  row.Index,
							Values: row.Fields,
						}
					}

					tableData := &database.TableData{
						Schema:   tableSchema,
						Rows:     rowData,
						Language: language,
					}
					if err := bulkInserter.InsertTableData(context.Background(), tableData); err != nil {
						slog.Error("Database insert failed", "table", tableName, "error", err)
						slog.Error("Failed to insert records", "table", tableName, "error", err)
						stats.DatabaseErrors++
						continue
					}

					stats.RowsInserted += int64(len(parsedTable.Rows))
				}

			}
			stats.ProcessedTables++
		}

		progress.Finish()
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

func getTableSchemas(validTables []dat.TableSchema, allTables bool, configuredTables []string) []dat.TableSchema {
	// If all_tables is true, return all valid tables
	if allTables || len(configuredTables) == 0 {
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
